import os
import glob
import asf_search as asf
import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
from modules.auth_base import EarthdataAuth

class S1SLCManager(EarthdataAuth):
    """
    Manages Sentinel-1 SLC data acquisition and preparation for InSAR processing.

    This class serves as the 'Provider' in the pipeline architecture. It handles:
    1. Searching images via ASF API or Scanning local directories.
    2. Filtering scenes by Path/Frame and managing the Master/Slave selection.
    3. Downloading files securely using Earthdata credentials.
    4. Exporting its status via 'get_status()' for the ISCEProcessor.

    Attributes:
        data_dir (Path): Directory where SLC files are stored/downloaded.
        roi (str): Well-Known Text (WKT) string defining the Region of Interest.
        search_df (pd.DataFrame): DataFrame containing metadata of all found scenes.
        compatible_df (pd.DataFrame): DataFrame filtered by the Master's Path/Track.
        stack_df (pd.DataFrame): DataFrame containing baseline information (B_perp, B_temp).
        master_idx (int): Index of the selected Master scene.
        selected_indices (set): Set of indices selected for processing (Slaves).
        downloaded_files (list): List of filenames successfully downloaded or verified.
    """

    def __init__(self, roi_wkt=None, data_dir="raw_data"):
        """
        Initialize the S1SLCManager.

        Args:
            roi_wkt (str, optional): Well-Known Text (WKT) string for API search.
                                     Can be None if only using 'scan_local_directory'.
            data_dir (str): Directory path where SLC files will be downloaded or scanned.
                            Defaults to "raw_data".
        """
        super().__init__()
        self.roi = roi_wkt
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.results = []            
        self.search_df = None        
        self.compatible_df = None    
        self.stack_df = None         
        
        self.master_idx = None
        self.master_scene = None
        self.selected_indices = set()
        self.downloaded_files = [] 

    # --------------------------------------------------------------------------
    # 0. Interface for Loose Coupling
    # --------------------------------------------------------------------------
    def get_status(self):
        """
        Returns the current status and configuration of the manager.
        
        This method allows external processors (like ISCEProcessor) to retrieve
        paths and configuration without accessing internal attributes directly.

        Returns:
            dict: A dictionary containing module status, data directory, master ID, and pairs.
        """
        pairs_list = self.get_pairs(full_path=True)
        master_id = self.compatible_df.loc[self.master_idx, 'Scene ID'] if self.master_idx is not None else None
        
        # [FIX] Master Index가 선택 목록에 포함되어 있을 경우를 대비해 제외하고 카운트
        actual_slaves = [i for i in self.selected_indices if i != self.master_idx]
        
        return {
            "module_name": "S1SLCManager",
            "is_ready": (self.master_idx is not None) and (len(actual_slaves) > 0), # 실제 Slave가 있어야 Ready
            "data_dir": str(self.data_dir.resolve()),
            "master_id": master_id,
            "slave_count": len(actual_slaves), # 수정된 카운트 반영
            "pairs": pairs_list
        }
    # --------------------------------------------------------------------------
    # 1. Search & Local Scan
    # --------------------------------------------------------------------------
    def search_images(self, start_date, end_date, orbit_direction=None):
        """
        Search for Sentinel-1 SLC images using the ASF API.

        Args:
            start_date (str): Start date string (Format: 'YYYY-MM-DD').
            end_date (str): End date string (Format: 'YYYY-MM-DD').
            orbit_direction (str, optional): Flight direction ('ASCENDING', 'DESCENDING', or None).

        Returns:
            pd.DataFrame: A DataFrame containing metadata of found scenes.
                          Returns None if the search fails.
        """
        direction_log = orbit_direction if orbit_direction else "ALL"
        print(f"[Search] API Search Sentinel-1 SLC ({start_date} ~ {end_date}) | {direction_log}...")
        
        try:
            self.results = asf.geo_search(
                platform=asf.PLATFORM.SENTINEL1,
                intersectsWith=self.roi,
                start=start_date,
                end=end_date,
                flightDirection=orbit_direction,
                processingLevel=asf.PRODUCT_TYPE.SLC,
                beamMode=asf.BEAMMODE.IW
            )
            self._results_to_df()
            return self.compatible_df
        except Exception as e:
            print(f"[Error] API Search failed: {e}")
            return None

    def scan_local_directory(self, dir_path=None):
        """
        Scan a local directory for existing Sentinel-1 SLC zip files.
        
        This allows creating a dataset without using the ASF API.
        Note: Strict Path/Track filtering and B_perp calculation are limited
        because full metadata is not extracted from the zip filename.

        Args:
            dir_path (str, optional): Path to the directory containing .zip files.
                                      If None, uses self.data_dir.

        Returns:
            pd.DataFrame: A DataFrame constructed from local files.
        """
        target_dir = Path(dir_path) if dir_path else self.data_dir
        
        if not target_dir.exists():
            print(f"[Error] Directory not found: {target_dir}")
            return

        # Matches standard S1 naming convention (S1A, S1B, S1C...)
        zip_files = sorted(list(target_dir.glob("S1*_IW_SLC__*.zip")))
        print(f"[Scan] Found {len(zip_files)} SLC files in '{target_dir}'")

        data = []
        for i, f in enumerate(zip_files):
            try:
                # Parse filename: S1A_IW_SLC__1SDV_20220101T000000_...
                fname = f.name
                parts = fname.split('_')
                # parts[0]=Platform, parts[5]=StartDateTime (standard format)
                platform = parts[0]
                date_str = parts[5][:8] # YYYYMMDD
                dt = pd.to_datetime(date_str)
                
                data.append({
                    'Index': i,
                    'Date': dt,
                    'Scene ID': fname, 
                    'Orbit': -1,       # Unknown in local mode
                    'Path': -1,        # Unknown in local mode
                    'Frame': -1,       # Unknown in local mode
                    'Flight Dir': 'Unknown',
                    'Platform': platform,
                    'Local Path': str(f)
                })
            except Exception as e:
                print(f"[Warning] Skipping malformed file: {f.name} ({e})")

        if not data:
            print("[Scan] No valid Sentinel-1 SLCs found.")
            return

        self.search_df = pd.DataFrame(data).set_index('Index')
        self.compatible_df = self.search_df.copy()
        
        # Create MockResult objects to maintain compatibility with other methods
        class MockResult:
            def __init__(self, props): self.properties = props
            
        self.results = [MockResult({'fileID': d['Scene ID'], 'fileName': d['Scene ID'], 'url': '', 'startTime': d['Date']}) 
                        for d in data]
        
        print("[Scan] Local database populated. (Note: Path/Orbit info is limited)")
        return self.compatible_df

    def _results_to_df(self):
        """Internal helper to convert ASF API results to DataFrame."""
        data = []
        for i, res in enumerate(self.results):
            props = res.properties
            data.append({
                'Index': i,
                'Date': pd.to_datetime(props['startTime']),
                'Scene ID': props['fileID'],
                'Orbit': props['orbit'],
                'Path': props.get('pathNumber'),    
                'Frame': props.get('frameNumber'),
                'Flight Dir': props['flightDirection'],
                'Platform': props.get('platform', 'S1').replace('Sentinel-1', 'S1'),
                'Local Path': None
            })
        
        self.search_df = pd.DataFrame(data).set_index('Index')
        self.compatible_df = self.search_df.copy()
        print(f"[Search] Found {len(self.results)} scenes.")

    # --------------------------------------------------------------------------
    # 2. Master & Pairing
    # --------------------------------------------------------------------------
    def set_master(self, index):
        """
        Set the Master image, filter compatible scenes, and calculate temporal baseline.

        Args:
            index (int): Index of the desired master scene in search_df.

        Returns:
            pd.DataFrame: Filtered DataFrame containing only compatible scenes (same Path).
        """
        if index not in self.search_df.index:
            print(f"[Error] Index {index} not found.")
            return
        
        self.master_idx = index
        self.master_scene = self.results[index]
        
        # 1. Filter by Path (if metadata is available)
        master_path = self.search_df.loc[index, 'Path']
        if master_path != -1:
            self.compatible_df = self.search_df[self.search_df['Path'] == master_path].copy()
            filtered = len(self.search_df) - len(self.compatible_df)
            print(f"[Master] Set to Idx {index}. Filtered {filtered} incompatible scenes (Path {master_path}).")
        else:
            print(f"[Master] Set to Idx {index}. (Local Mode: Path filtering skipped)")
            self.compatible_df = self.search_df.copy()

        # 2. Calculate Temporal Baseline immediately
        master_date = self.compatible_df.loc[self.master_idx, 'Date']
        self.compatible_df['B_temp_days'] = (self.compatible_df['Date'] - master_date).dt.days
        
        # Initialize B_perp_m with NaN (will be filled by get_stack_info later)
        if 'B_perp_m' not in self.compatible_df.columns:
            self.compatible_df['B_perp_m'] = float('nan')

        return self.compatible_df

    def unset_master(self):
        """
        Unset the current Master scene and reset filters.

        Returns:
            pd.DataFrame: The restored full DataFrame with all scenes.
        """
        if self.master_idx is None:
            print("[Info] Master is already unset.")
            return

        print(f"[Master] Unsetting Master (Index {self.master_idx})...")
        self.master_idx = None
        self.master_scene = None
        self.stack_df = None 
        
        # Restore compatible_df to full list
        self.compatible_df = self.search_df.copy()
        
        print(f"[Reset] List restored. View type reverted to 'Selected'.")
        return self.compatible_df

    def get_master(self):
        """
        Retrieve information about the currently selected Master.

        Returns:
            pd.DataFrame: Single-row DataFrame containing Master info, or None.
        """
        if self.master_idx is None: return None
        return pd.DataFrame(self.search_df.loc[self.master_idx]).rename(columns={self.master_idx: 'Master Info'})

    def get_pairs(self, full_path=False):
        """
        Generate (Master, Slave) filename tuples for ISCE processing.

        Args:
            full_path (bool): If True, returns absolute paths. If False, returns filenames.

        Returns:
            list: List of tuples, e.g., [('Master.zip', 'Slave1.zip'), ...].
                  Returns empty list if Master or Slaves are not selected.
        """
        if self.master_idx is None:
            # print("[Error] No Master set.") # Silent return for get_status check
            return []
            
        if not self.selected_indices:
            # print("[Error] No Slaves selected.") # Silent return
            return []

        master_name = self.compatible_df.loc[self.master_idx, 'Scene ID']
        if not master_name.endswith('.zip'): master_name += '.zip'
        
        pairs = []
        for idx in sorted(list(self.selected_indices)):
            if idx == self.master_idx: continue
            
            slave_name = self.compatible_df.loc[idx, 'Scene ID']
            if not slave_name.endswith('.zip'): slave_name += '.zip'
            
            if full_path:
                m_path = str(self.data_dir.resolve() / master_name)
                s_path = str(self.data_dir.resolve() / slave_name)
                pairs.append((m_path, s_path))
            else:
                pairs.append((master_name, slave_name))
            
        return pairs

    # --------------------------------------------------------------------------
    # 3. Baseline Analysis
    # --------------------------------------------------------------------------
    def get_stack_info(self):
        """
        Calculate full baseline information (Perpendicular + Temporal).
        
        Note: In Local Scan mode, B_perp cannot be calculated accurately without
        full metadata, so it defaults to 0.0.

        Returns:
            pd.DataFrame: DataFrame with 'B_perp_m' and 'B_temp_days' columns.
        """
        if self.master_idx is None:
            print("[Warning] Set master first.")
            return None
        
        # Check for Local Mode (Path == -1)
        is_local = (self.search_df.iloc[0]['Path'] == -1)

        if is_local:
            print("[Warning] Local Mode: B_perp cannot be calculated via API.")
            print("          Setting B_perp_m to 0.0.")
            self.compatible_df['B_perp_m'] = 0.0
            self.stack_df = self.compatible_df
            return self.stack_df

        print(f"[Baseline] Calculating B_perp for {len(self.compatible_df)} scenes...")
        try:
            stack_results = asf.baseline_search.stack_from_product(self.master_scene)
            baseline_map = {item.properties['fileID']: item.properties['perpendicularBaseline'] 
                            for item in stack_results if 'perpendicularBaseline' in item.properties}
            
            df = self.compatible_df.copy()
            df['B_perp_m'] = df['Scene ID'].map(baseline_map).fillna(0)
            
            # Recalculate B_temp to be safe
            master_date = df.loc[self.master_idx, 'Date']
            df['B_temp_days'] = (df['Date'] - master_date).dt.days
            
            self.stack_df = df
        except Exception as e:
            print(f"[Error] Baseline calculation failed: {e}")
            self.stack_df = self.compatible_df
            
        return self.stack_df

    # --------------------------------------------------------------------------
    # 4. Selection & Download
    # --------------------------------------------------------------------------
    def add_selected(self, indices):
        """
        Add indices to the selection set.

        Args:
            indices (int or list): Index (or list of indices) to select.
        """
        if isinstance(indices, int): indices = [indices]
        for idx in indices:
            if idx in self.compatible_df.index:
                self.selected_indices.add(idx)
                print(f"[Select] Added Index {idx}")
            else:
                print(f"[Warning] Index {idx} invalid.")

    def remove_selected(self, indices):
        """
        Remove indices from the selection set.

        Args:
            indices (int or list): Index (or list of indices) to deselect.
        """
        if isinstance(indices, int): indices = [indices]
        for idx in indices:
            self.selected_indices.discard(idx)
            print(f"[Select] Removed Index {idx}")

    def purge_selected(self):
        """Clear all selected indices."""
        self.selected_indices.clear()
        print("[Select] Selection cleared.")

    def view_selected(self):
        """
        Generate a DataFrame view of the current selection.
        Includes Type (Master/Slave) and Baseline info if available.

        Returns:
            pd.DataFrame: View of selected scenes.
        """
        all_idxs = self.selected_indices.copy()
        if self.master_idx is not None: all_idxs.add(self.master_idx)
        
        if not all_idxs: return pd.DataFrame()
        
        # Prefer stack_df if available
        source = self.stack_df if self.stack_df is not None else self.compatible_df
        
        valid_idxs = [i for i in sorted(list(all_idxs)) if i in source.index]
        view_df = source.loc[valid_idxs].copy()
        
        type_col = []
        for idx in view_df.index:
            # 1. Master Case
            if self.master_idx is not None and idx == self.master_idx:
                type_col.append("MASTER")
            
            # 2. Slave Case
            elif self.master_idx is not None:
                parts = []
                
                # Check Temporal Baseline
                if 'B_temp_days' in view_df.columns and pd.notnull(view_df.loc[idx, 'B_temp_days']):
                      parts.append(f"{int(view_df.loc[idx, 'B_temp_days']):+d}d")
                
                # Check Perpendicular Baseline
                if 'B_perp_m' in view_df.columns and pd.notnull(view_df.loc[idx, 'B_perp_m']):
                    parts.append(f"{view_df.loc[idx, 'B_perp_m']:.1f}m")
                
                info_str = f"({', '.join(parts)})" if parts else ""
                type_col.append(f"Slave {info_str}")
            
            # 3. No Master Case
            else:
                type_col.append("Selected")
                
        view_df.insert(0, 'Type', type_col)
        return view_df

    def download_selected(self, download_dir=None):
        """
        Download or verify selected scenes.

        Args:
            download_dir (str, optional): Directory path to save/check files.
                                          If None, uses self.data_dir.

        Returns:
            list: List of valid filenames found or downloaded.
        """
        target_dir = Path(download_dir) if download_dir else self.data_dir
        
        to_dl = self.selected_indices.copy()
        if self.master_idx is not None: to_dl.add(self.master_idx)
        
        if not to_dl:
            print("[Error] Nothing to download.")
            return []
        
        # Check mode based on Path metadata
        is_local_mode = (self.search_df.iloc[0]['Path'] == -1)

        if is_local_mode:
            print(f"[Download] Local Mode: Verifying files in '{target_dir}'...")
            final_list = []
            for idx in to_dl:
                fname = self.search_df.loc[idx, 'Scene ID']
                if not fname.endswith('.zip'): fname += '.zip'
                
                fpath = target_dir / fname
                if fpath.exists():
                    final_list.append(fname)
                else:
                    print(f"[Error] Missing local file: {fname}")
            self.downloaded_files = final_list
            return final_list

        # API Mode (Sequential Download)
        if not self.authenticate(): return []
        os.makedirs(target_dir, exist_ok=True)
        session = self.get_session()
        
        target_scenes = [self.results[i] for i in sorted(list(to_dl))]
        print(f"\n[Download] Starting Sequential Download ({len(target_scenes)} files) to {target_dir}...")
        
        downloaded = []
        for idx, scene in enumerate(target_scenes):
            url = scene.properties['url']
            filename = scene.properties['fileName']
            file_path = target_dir / filename
            step = f"[{idx+1}/{len(target_scenes)}]"

            if file_path.exists():
                print(f"{step} Found existing file: {filename}")
                downloaded.append(filename)
                continue

            print(f"{step} Downloading: {filename}")
            try:
                with session.get(url, stream=True) as response:
                    response.raise_for_status()
                    total = int(response.headers.get('content-length', 0))
                    # tqdm: ascii=True for Jupyter compatibility
                    with tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024, ncols=80, ascii=True, desc="   Progress") as pbar:
                        with open(file_path, 'wb') as f:
                            for chunk in response.iter_content(5*1024*1024):
                                if chunk:
                                    size = f.write(chunk)
                                    pbar.update(size)
                downloaded.append(filename)
                print("   Complete.\n")
            except Exception as e:
                print(f"   Failed: {e}\n")
                if file_path.exists(): os.remove(file_path)

        self.downloaded_files = downloaded
        return downloaded

    # --------------------------------------------------------------------------
    # 5. Visualization
    # --------------------------------------------------------------------------
    def plot_baseline(self, days=None, interactive=True):
        """
        Plot Perpendicular vs Temporal Baseline graph.

        Args:
            days (int, optional): Time range in days to zoom around master.
            interactive (bool): Use Plotly if True, Matplotlib if False.
        """
        # Use stack_df if exists, otherwise compatible_df (for B_temp only plots)
        source_df = self.stack_df if self.stack_df is not None else self.compatible_df
        
        if source_df is None or 'B_temp_days' not in source_df.columns:
             print("[Warning] Master not set or Baselines not calculated.")
             return

        master_row = source_df.loc[self.master_idx]
        slaves = source_df.drop(self.master_idx)
        
        if days:
            x_min = master_row['Date'] - pd.Timedelta(days=days)
            x_max = master_row['Date'] + pd.Timedelta(days=days * 0.1)
        else:
            x_min, x_max = None, None

        if interactive:
            fig = go.Figure()
            # Support colors for S1A, S1B, S1C
            colors = slaves['Platform'].map(lambda x: {'S1A': 'royalblue', 'S1B': 'forestgreen', 'S1C': 'orange'}.get(x, 'gray'))
            
            # Handle NaN B_perp in Local Mode
            hover_txt = []
            for i, r in slaves.iterrows():
                bp_str = f"{r['B_perp_m']:.1f}m" if pd.notnull(r.get('B_perp_m')) else "N/A"
                hover_txt.append(f"Idx: {i}<br>{r['Date'].date()}<br>B_perp: {bp_str}")

            # Plot Slaves
            fig.add_trace(go.Scatter(
                x=slaves['Date'], y=slaves.get('B_perp_m', [0]*len(slaves)), 
                mode='markers+text', marker=dict(size=10, color=colors),
                text=[f"{int(d):+d}d" for d in slaves['B_temp_days']], 
                textposition="top center", hovertext=hover_txt, hoverinfo="text", name="Slaves"
            ))
            # Plot Master
            fig.add_trace(go.Scatter(
                x=[master_row['Date']], y=[master_row.get('B_perp_m', 0)],
                mode='markers+text', marker=dict(symbol='star', size=18, color='red'),
                text=["MASTER"], textposition="bottom center", name="Master"
            ))
            
            fig.update_layout(
                title="Baseline Plot", 
                xaxis_range=[x_min, x_max] if days else None, 
                yaxis_title="Perpendicular Baseline (m)", 
                template="plotly_white"
            )
            fig.show()
        else:
            plt.figure(figsize=(10, 6))
            plt.scatter(slaves['Date'], slaves.get('B_perp_m', [0]*len(slaves)), c='blue', label='Slaves')
            plt.scatter(master_row['Date'], master_row.get('B_perp_m', 0), c='red', marker='*', s=200, label='Master')
            if days: plt.xlim(x_min, x_max)
            plt.grid(True, alpha=0.3)
            plt.title("Baseline Plot")
            plt.xlabel("Date")
            plt.ylabel("Perpendicular Baseline (m)")
            plt.legend()
            plt.show()