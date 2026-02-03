import os
import sys
import shutil
import subprocess
import logging
import lxml.etree as ET
import pandas as pd
import rasterio
from pathlib import Path
from shapely.geometry import box
from shapely.wkt import loads as load_wkt

class ISCEProcessor:
    """
    Automates the ISCE2 topsApp.py processing pipeline for InSAR workflows.

    This class provides a robust wrapper around the ISCE2 framework, addressing common
    environment issues and simplifying the execution process.

    Attributes:
        work_dir (Path): The working directory for processing.
        raw_data_dir (Path): Directory containing raw SLC data.
        dem_path (Path): Path to the DEM file.
        logger (logging.Logger): Logger instance for capturing execution details.
    """

    def __init__(self, work_dir="process_insar", raw_data_dir=None, dem_path=None):
        """
        Initializes the ISCEProcessor with workspace paths and logging configuration.

        Args:
            work_dir (str): Directory where processing outputs and logs will be stored.
            raw_data_dir (str, optional): Path to the directory containing raw SLC zip files.
            dem_path (str, optional): Path to the Digital Elevation Model (DEM) file.
        """
        # 1. Initialize Paths
        self.work_dir = Path(work_dir).resolve()
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.xml_path = self.work_dir / "topsApp.xml"
        
        self.raw_data_dir = Path(raw_data_dir) if raw_data_dir else None
        self.dem_path = Path(dem_path) if dem_path else None

        # 2. Configure Logger
        self.logger = logging.getLogger("ISCEProcessor")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # 3. Patch Environment (Critical Fix for imageMath.py)
        self._patch_environment()

        self.logger.info(f"Preparing workspace in: {self.work_dir}")

    def _patch_environment(self):
        """
        Diagnoses and patches the execution environment to ensure ISCE2 stability.

        Critical Fixes:
        1. **Path Correction**: Injects the current python `bin` directory into `PATH`.
        2. **PROJ_LIB**: Sets the `PROJ_LIB` environment variable to suppress GDAL warnings.
        3. **Tool Collision Fix**: Locates the *REAL* `imageMath.py` inside the `isce.applications`
           package (site-packages) and creates a symbolic link in `bin/`. This bypasses the
           broken wrapper script often created by Conda, which causes runtime errors.
        """
        self.logger.info("🔧 Checking and patching environment for ISCE2...")

        bin_dir = Path(os.path.dirname(sys.executable))
        
        # 1. Add bin to PATH
        current_path = os.environ.get('PATH', '')
        if str(bin_dir) not in current_path.split(os.pathsep):
            os.environ['PATH'] = f"{bin_dir}{os.pathsep}{current_path}"
            self.logger.info(f"   -> Added {bin_dir} to PATH")

        # 2. Set PROJ_LIB (Suppress GDAL Warnings)
        proj_lib = bin_dir.parent / 'share' / 'proj'
        if proj_lib.exists():
            os.environ['PROJ_LIB'] = str(proj_lib)

        # 3. Fix Tool Collisions (The Real Fix)
        # We must look inside the site-packages, NOT the bin directory.
        try:
            import isce
            isce_pkg_dir = Path(os.path.dirname(isce.__file__))
            isce_apps_dir = isce_pkg_dir / "applications" # Source of truth
        except ImportError:
            self.logger.error("❌ ISCE package not found. Is it installed?")
            return

        tool_map = {
            "imageMath.py": "imageMath.py",
            "imageStitch.py": "imageStitch.py"
        }

        for link_name, source_name in tool_map.items():
            src_path = isce_apps_dir / source_name
            dst_path = bin_dir / link_name

            if src_path.exists():
                # Force create/overwrite symlink
                try:
                    if dst_path.exists() or dst_path.is_symlink():
                        dst_path.unlink()
                    os.symlink(src_path, dst_path)
                    self.logger.info(f"   -> Fixed symlink: {link_name} -> ISCE Internal App")
                except OSError as e:
                    self.logger.warning(f"   -> Failed to link {link_name}: {e}")
            else:
                self.logger.error(f"   -> CRITICAL: Internal tool not found: {src_path}")

    def create_config(self, slc_status, orbit_status, dem_status, 
                      roi_wkt=None, slc_bbox=None, 
                      unwrapper="snaphu", use_gpu=True):
        """
        Generates the 'topsApp.xml' configuration file.

        This method handles filename auto-correction (adding/removing -SLC suffixes),
        calculates the intersection between the DEM and ROI, and writes the ISCE2 configuration.

        Args:
            slc_status (dict): Status dict containing SLC pair paths (master, slave).
            orbit_status (dict): Status dict containing orbit/aux directories.
            dem_status (dict): Status dict containing DEM path.
            roi_wkt (str, optional): Region of Interest in WKT format.
            slc_bbox (list, optional): Bounding box [S, N, W, E].
            unwrapper (str): Unwrapping method ('snaphu' or 'icu').
            use_gpu (bool): Enable GPU acceleration if available.
        """
        
        # Helper: Robust Path Resolver
        def resolve_real_path(input_path_str):
            path = Path(input_path_str)
            if path.exists(): return path.resolve()
            
            # Heuristic 1: Remove "-SLC" suffix
            if "-SLC.zip" in path.name:
                new_path = path.with_name(path.name.replace("-SLC.zip", ".zip"))
                if new_path.exists(): 
                    self.logger.warning(f"⚠️ [Auto-Fix] Filename mismatch: {path.name} -> {new_path.name}")
                    return new_path.resolve()
            
            # Heuristic 2: Add "-SLC" suffix
            if not "-SLC.zip" in path.name and path.suffix == ".zip":
                new_path = path.with_name(path.stem + "-SLC.zip")
                if new_path.exists(): 
                    self.logger.warning(f"⚠️ [Auto-Fix] Filename mismatch (Added -SLC): {path.name} -> {new_path.name}")
                    return new_path.resolve()

            self.logger.error(f"❌ File not found: {path}")
            return path 

        # 1. Resolve Paths & Create Symlinks
        master_path = resolve_real_path(slc_status['pairs'][0][0])
        slave_path = resolve_real_path(slc_status['pairs'][0][1])

        self._create_symlink(master_path, self.work_dir / master_path.name)
        self._create_symlink(slave_path, self.work_dir / slave_path.name)

        dem_src = Path(dem_status['dem_path'])
        self._create_symlink(dem_src, self.work_dir / "dem.wgs84")
        
        # Link DEM XML if it exists
        dem_xml_src = dem_src.with_suffix(".wgs84.xml")
        if dem_xml_src.exists():
            self._create_symlink(dem_xml_src, self.work_dir / "dem.wgs84.xml")

        # 2. ROI
        final_bbox = self._calculate_roi_bounds(self.work_dir / "dem.wgs84", roi_wkt, slc_bbox)

        # 3. Build XML Structure
        root = ET.Element("topsApp")
        topsinsar = ET.SubElement(root, "component", name="topsinsar")

        # Configure Components (Reference/Secondary)
        for comp_name, path in [("reference", master_path), ("secondary", slave_path)]:
            comp = ET.SubElement(topsinsar, "component", name=comp_name)
            self._add_property(comp, "safe", f"['{str(path.resolve())}']")
            self._add_property(comp, "output directory", comp_name)
            self._add_property(comp, "orbit directory", orbit_status['orbit_dir'])
            self._add_property(comp, "auxiliary data directory", orbit_status['aux_dir'])

        # Global Properties
        self._add_property(topsinsar, "dem filename", "dem.wgs84")
        
        if final_bbox:
            self._add_property(topsinsar, "region of interest", str(final_bbox))

        self._add_property(topsinsar, "swaths", "[1, 2, 3]")
        self._add_property(topsinsar, "do unwrap", "True")
        self._add_property(topsinsar, "unwrapper name", unwrapper)
        
        if use_gpu:
            self._add_property(topsinsar, "useGPU", "True")

        # Explicit Geocode List
        geocode_files = [
            'merged/phsig.cor', 'merged/filt_topophase.unw', 'merged/los.rdr', 
            'merged/topophase.flat', 'merged/filt_topophase.flat', 
            'merged/topophase.cor', 'merged/z.rdr',
            'merged/lat.rdr', 'merged/lon.rdr'
        ]
        self._add_property(topsinsar, "geocode list", str(geocode_files))

        # Write XML file
        tree = ET.ElementTree(root)
        with open(self.xml_path, "wb") as f:
            tree.write(f, pretty_print=True, xml_declaration=True, encoding='utf-8')
        
        self.logger.info(f"✅ Config generated: {self.xml_path.name}")

    def run_process(self, start_step=None, end_step=None):
        """
        Locates and executes the 'topsApp.py' script.

        Args:
            start_step (str, optional): The ISCE step to start from (e.g., 'unwrap').
            end_step (str, optional): The ISCE step to end at.
        """
        target_script = None
        
        # Priority 1: ISCE Package
        try:
            import isce
            candidate = Path(os.path.dirname(isce.__file__)) / "applications" / "topsApp.py"
            if candidate.exists(): target_script = str(candidate)
        except ImportError: pass

        # Priority 2: Conda Bin
        if not target_script:
            candidate = Path(sys.prefix) / "bin" / "topsApp.py"
            if candidate.exists(): target_script = str(candidate)

        if not target_script:
            self.logger.error("❌ Could not find topsApp.py. Please check ISCE installation.")
            return

        cmd = [sys.executable, target_script] 
        if start_step: cmd.append(f"--start={start_step}")
        if end_step: cmd.append(f"--end={end_step}")

        self.logger.info(f"Executing: {' '.join(cmd)}")
        self._execute_command(cmd)

    def _execute_command(self, command):
        """
        Executes a shell command with real-time Tee logging (Console + File).
        Ensures output is captured even if the Jupyter kernel disconnects.
        """
        log_file_path = self.work_dir / "isce_execution.log"
        
        try:
            env = os.environ.copy()
            
            # Open the log file in append mode
            with open(log_file_path, "a", encoding="utf-8") as log_f:
                log_f.write(f"\n\n{'='*20}\nEXECUTION START: {' '.join(command)}\n{'='*20}\n")
                
                process = subprocess.Popen(
                    command,
                    cwd=str(self.work_dir),
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, # Merge stderr into stdout
                    text=True,
                    bufsize=1 # Line-buffered for real-time output
                )
                
                # Real-time streaming
                for line in process.stdout:
                    stripped_line = line.strip()
                    print(f"  | {stripped_line}")  # To Jupyter Console
                    log_f.write(f"{stripped_line}\n") # To Log File
                    log_f.flush() # Force write to disk immediately
                
                process.wait()
                
                if process.returncode == 0:
                    self.logger.info("✅ Step completed.")
                    log_f.write("\n[SUCCESS] Step completed.\n")
                else:
                    self.logger.error(f"❌ Failed (Code {process.returncode}).")
                    log_f.write(f"\n[FAILURE] Process exited with code {process.returncode}.\n")
                    
        except Exception as e:
            self.logger.error(f"❌ Execution error: {e}")

    # ==========================================================================
    # [Restored] Result Management Methods
    # ==========================================================================

    def get_results(self, geocoded_only=False, as_df=True):
        """
        Scans the output directory (merged/) and lists available product files.

        Args:
            geocoded_only (bool): If True, lists only geocoded files (.geo).
            as_df (bool): If True, returns a Pandas DataFrame. Otherwise, a list of Paths.
        
        Returns:
            pd.DataFrame or list: Collection of available output files.
        """
        merged_dir = self.work_dir / "merged"
        if not merged_dir.exists():
            self.logger.warning("⚠️ 'merged' directory not found. Process might not have finished.")
            return pd.DataFrame() if as_df else []

        # Define extensions to look for
        base_extensions = ['.unw', '.cor', '.flat', '.rdr']
        if geocoded_only:
            target_exts = [f"{ext}.geo" for ext in base_extensions]
        else:
            target_exts = base_extensions + [f"{ext}.geo" for ext in base_extensions]

        found_files = []
        for f in merged_dir.iterdir():
            # Check if file ends with any target extension and ignore metadata files
            if any(str(f).endswith(ext) for ext in target_exts) and f.suffix not in ['.xml', '.vrt']:
                found_files.append({
                    "Filename": f.name,
                    "Type": "Geocoded" if ".geo" in f.name else "Radar Coords",
                    "Size (MB)": round(f.stat().st_size / (1024 * 1024), 2),
                    "Path": str(f)
                })

        if as_df:
            df = pd.DataFrame(found_files)
            if not df.empty:
                df = df.sort_values(by="Filename").reset_index(drop=True)
            return df
        else:
            return [Path(f["Path"]) for f in found_files]

    def load_raster(self, filename):
        """
        Loads a specific ISCE product file into a rasterio dataset.
        Automatically handles VRT linking for binary files.

        Args:
            filename (str): Name of the file (e.g., 'filt_topophase.unw.geo').
        
        Returns:
            rasterio.io.DatasetReader: Open rasterio dataset handle.
        """
        target_path = self.work_dir / "merged" / filename
        
        # If specific file not found, try searching
        if not target_path.exists():
            candidates = list((self.work_dir / "merged").glob(f"{filename}*"))
            # Filter out xml/vrt unless explicitly asked
            candidates = [c for c in candidates if c.suffix not in ['.xml', '.vrt']]
            
            if candidates:
                target_path = candidates[0]
                self.logger.info(f"🔍 Found closest match: {target_path.name}")
            else:
                raise FileNotFoundError(f"❌ Could not find file: {filename} in merged/")

        self.logger.info(f"📂 Loading raster: {target_path.name}")
        
        # ISCE creates binary files paired with .vrt headers. 
        # Rasterio needs the .vrt to understand the binary structure.
        vrt_path = target_path.with_suffix(target_path.suffix + ".vrt")
        if vrt_path.exists():
            return rasterio.open(vrt_path)
        else:
            # Try opening directly (some files might be self-contained or different format)
            return rasterio.open(target_path)

    # ==========================================================================
    # Internal Helpers
    # ==========================================================================

    def _calculate_roi_bounds(self, dem_path, roi_wkt, slc_bbox):
        """
        Internal helper to calculate the intersection between the DEM and ROI.
        
        Args:
            dem_path (Path): Path to the DEM file.
            roi_wkt (str): ROI in WKT format.
            slc_bbox (list): SLC bounding box [S, N, W, E].

        Returns:
            list: Bounding box [S, N, W, E] of the intersection, or None.
        """
        try:
            with rasterio.open(dem_path) as src:
                dem_poly = box(*src.bounds)
            
            target_poly = None
            if roi_wkt:
                self.logger.info("[ROI] Using user-provided WKT.")
                target_poly = load_wkt(roi_wkt)
            elif slc_bbox:
                self.logger.info("[ROI] Using SLC intersection bbox.")
                target_poly = box(slc_bbox[2], slc_bbox[0], slc_bbox[3], slc_bbox[1])
            else:
                return None

            intersection = target_poly.intersection(dem_poly)

            if intersection.is_empty:
                self.logger.warning("[ROI] ROI does not overlap with DEM! ROI set to None.")
                return None
            
            b = intersection.bounds 
            final_bbox = [b[1], b[3], b[0], b[2]]
            self.logger.info(f"[ROI] Intersection Calculated: {final_bbox}")
            return final_bbox

        except Exception as e:
            self.logger.warning(f"[ROI] Failed to calculate ROI intersection ({e}). Using input as is.")
            if slc_bbox: return slc_bbox
            return None

    def _create_symlink(self, src_path, link_path):
        """Creates a symbolic link safely, removing existing ones if necessary."""
        src = Path(src_path).resolve()
        lnk = Path(link_path)
        if lnk.exists() or lnk.is_symlink():
            lnk.unlink()
        try:
            os.symlink(src, lnk)
        except OSError as e:
            self.logger.error(f"Failed to link {lnk.name}: {e}")

    def _add_property(self, parent, name, value):
        """Helper to add a property element to the ISCE XML."""
        prop = ET.SubElement(parent, "property", name=name)
        val = ET.SubElement(prop, "value")
        val.text = str(value)