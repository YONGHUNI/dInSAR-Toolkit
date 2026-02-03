import os
import pandas as pd
from pathlib import Path
from modules.auth_base import EarthdataAuth

try:
    from eof.download import download_eofs
except ImportError:
    print("[SYSTEM] 'sentineleof' library is missing. Please install: pip install sentineleof")
    download_eofs = None

class OrbitManager(EarthdataAuth):
    """
    Manages the retrieval of Sentinel-1 Orbit files (POEORB/RESORB).

    This class wraps the 'sentineleof' library to download orbit files from NASA ASF.
    It inherits from EarthdataAuth to handle .netrc authentication automatically.
    It supports S1A, S1B, and the new S1C mission by parsing timestamps directly.

    Attributes:
        orbit_dir (Path): The directory path where orbit files (.EOF) are stored.
    """

    def __init__(self, orbit_dir="aux_orbits"):
        """
        Initializes the OrbitManager.

        Calls the parent EarthdataAuth class to ensure valid authentication exists,
        and creates the directory for storing orbit files.

        Args:
            orbit_dir (str): Path to the directory for saving orbit files. Defaults to "aux_orbits".
        """
        super().__init__()
        self.authenticate() 
        
        self.orbit_dir = Path(orbit_dir)
        self.orbit_dir.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
    # 0. Interface for Loose Coupling (Fixed)
    # --------------------------------------------------------------------------
    def get_status(self):
        """
        Returns the current status and configuration of the manager.
        
        This method allows external processors (like ISCEProcessor) to retrieve
        orbit directory paths and status without accessing internal attributes directly.

        Returns:
            dict: A dictionary containing module status, orbit directory, and file count.
        """
        # Count existing .EOF files
        if self.orbit_dir.exists():
            eof_count = len(list(self.orbit_dir.glob("*.EOF")))
        else:
            eof_count = 0
        
        return {
            "module_name": "OrbitManager",
            "is_ready": True,
            "orbit_dir": str(self.orbit_dir.resolve()),
            # [FIX] ISCEProcessor가 필요로 하는 aux_dir 추가 (Orbit 폴더와 동일하게 설정)
            "aux_dir": str(self.orbit_dir.resolve()), 
            "file_count": eof_count
        }

    # --------------------------------------------------------------------------
    # 1. Helper Methods
    # --------------------------------------------------------------------------
    def _get_timestamp_from_filename(self, filename):
        """
        Extracts the acquisition start timestamp (YYYYMMDDTHHMMSS) from a Sentinel-1 filename.

        This method is crucial for supporting Sentinel-1C, as passing the full filename
        to the 'sentineleof' library can cause parsing errors.

        Args:
            filename (str): The Sentinel-1 SLC filename (e.g., 'S1A_IW_SLC__1SDV_20220101T060000_...').

        Returns:
            str or None: The extracted timestamp string (e.g., '20220101T060000') or None if parsing fails.
        """
        try:
            parts = filename.split('_')
            # In the standard naming convention, the start timestamp is at index 5.
            timestamp = parts[5]
            if len(timestamp) == 15 and 'T' in timestamp:
                return timestamp
            return None
        except IndexError:
            return None

    def _get_mission_from_filename(self, filename):
        """
        Extracts the Mission ID (S1A/S1B/S1C) from a Sentinel-1 filename.

        Args:
            filename (str): The Sentinel-1 SLC filename.

        Returns:
            str or None: 'S1A', 'S1B', or 'S1C' if found; otherwise, None.
        """
        try:
            mission = filename.split('_')[0]
            if mission in ["S1A", "S1B", "S1C"]:
                return mission
            return None
        except IndexError:
            return None

    # --------------------------------------------------------------------------
    # 2. Main Logic
    # --------------------------------------------------------------------------
    def fetch_orbits(self, slc_files, precise_only=False):
        """
        Downloads orbit files for a list of Sentinel-1 SLC scenes.

        This method parses the acquisition date and mission from filenames and requests
        orbit files from the NASA ASF server. It prioritizes Precise Orbits (POE) and
        falls back to Restituted Orbits (RES) if POE is unavailable, unless 'precise_only' is True.

        Args:
            slc_files (list or str): A list of SLC file paths/names or a single file path string.
            precise_only (bool): If True, enforces "Strict Mode". If a Precise Orbit is not found
                                 and only a Restituted Orbit is downloaded, it is marked as Failed.

        Returns:
            pd.DataFrame: A report containing the status for each scene.
                Columns: ['Scene ID', 'Acq Date', 'Orbit Type', 'Status']
        """
        if download_eofs is None:
            print("[Error] 'sentineleof' library not loaded.")
            return pd.DataFrame()

        if isinstance(slc_files, (str, Path)):
            slc_files = [str(slc_files)]
        else:
            slc_files = [str(f) for f in slc_files]

        unique_files = sorted(list(set(slc_files)))
        
        results = []
        mode_str = "Strict (Precise Only)" if precise_only else "Auto Fallback (POE -> RES)"
        print(f"\n[OrbitManager] Checking orbits for {len(unique_files)} scenes...")
        print(f"               Target Dir: {self.orbit_dir}")
        print(f"               Mode: {mode_str}")

        for scene_path in unique_files:
            scene_name = Path(scene_path).name
            timestamp = self._get_timestamp_from_filename(scene_name)
            mission = self._get_mission_from_filename(scene_name)
            acq_date = timestamp[:8] if timestamp else "Unknown"
            
            status = "Unknown"
            orbit_type = "-"
            
            if timestamp is None or mission is None:
                results.append({
                    'Scene ID': scene_name, 'Acq Date': acq_date, 
                    'Orbit Type': '-', 'Status': '❌ Invalid Filename'
                })
                continue

            # Inner helper: Verify if the downloaded file is POE or RES
            def determine_orbit_type(downloaded_paths):
                """Determines if the downloaded file is Precise (POE) or Restituted (RES)."""
                if not downloaded_paths:
                    return None, None
                
                filename = str(downloaded_paths[0])
                
                if "POEORB" in filename:
                    return "Precise (POE)", "✅ Success"
                elif "RESORB" in filename:
                    return "Restituted (RES)", "✅ Success (RES Found)"
                else:
                    return "Unknown Type", "✅ Success"

            try:
                # 1. Strategy A: Attempt to download Precise Orbit (POEORB)
                orbits = download_eofs(
                    orbit_dts=[timestamp],
                    missions=[mission],
                    save_dir=str(self.orbit_dir),
                    orbit_type='precise'
                )
                
                if orbits:
                    orbit_type, status = determine_orbit_type(orbits)
                    
                    # [Strict Mode Check]
                    if precise_only and "Restituted" in orbit_type:
                        status = "❌ Failed (Strict Mode: Only RES found)"
                        # Optional: os.remove(orbits[0]) 
                
                else:
                    # 2. Strategy B: Fallback Logic (if POE failed)
                    if precise_only:
                        status = "❌ Failed (Precise Missing)"
                        orbit_type = "None"
                    else:
                        print(f"   [Info] POE request returned empty for {acq_date}. Trying RESORB...")
                        orbits_res = download_eofs(
                            orbit_dts=[timestamp],
                            missions=[mission],
                            save_dir=str(self.orbit_dir),
                            orbit_type='restituted'
                        )
                        
                        if orbits_res:
                            orbit_type, status = determine_orbit_type(orbits_res)
                            status = "⚠️ Fallback" 
                        else:
                            status = "❌ Failed (Both Missing)"
                            orbit_type = "None"
            
            except Exception as e:
                err_msg = str(e)
                status = f"❌ Error: {err_msg}"
                
                # Attempt fallback to RESORB even on error
                if not precise_only and "restituted" not in err_msg:
                     try:
                        orbits_res = download_eofs(
                            orbit_dts=[timestamp],
                            missions=[mission],
                            save_dir=str(self.orbit_dir),
                            orbit_type='restituted'
                        )
                        if orbits_res:
                            orbit_type, _ = determine_orbit_type(orbits_res)
                            status = "⚠️ Fallback (After Error)"
                     except:
                         pass
            
            results.append({
                'Scene ID': scene_name,
                'Acq Date': acq_date,
                'Orbit Type': orbit_type,
                'Status': status
            })

        return pd.DataFrame(results)

    def get_orbit_dir(self):
        """Returns the path to the orbit directory."""
        return self.orbit_dir