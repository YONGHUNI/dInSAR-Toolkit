import os
import logging
from shapely.wkt import loads as load_wkt

try:
    from modules.SLC_manager import SLCManager
    from modules.orbit_manager import OrbitManager
    from modules.DEM_manager import DEMManager
    from modules.isce_processor import ISCEProcessor
    print(" Successfully imported custom modules from './modules/'.")
except ImportError as e:
    print(f" ImportError: {e}")
    print("   Please check if the file names and class names inside 'modules/' match.")
    exit(1)

# ==============================================================================
# 1. Configuration (Global Settings)
# ==============================================================================

# Region of Interest (ROI) in WKT format
# Target: North Korea-China border region
ROI_WKT = "POLYGON ((129.2 42.2, 129.3 42.2, 129.3 42.3, 129.2 42.3, 129.2 42.2))"

# Date Range for Analysis (Recent 12-day interval)
START_DATE = "2025-12-20"
END_DATE = "2026-01-10"

# Workspace Directory
WORK_DIR = "./insar_workspace"

# Critical Optimization Parameters
# DEM_BUFFER: Prepare DEM 0.2 deg larger than the full SLC extent to prevent edge noise.
# LOOKS: Multilooking factors to suppress speckle noise (Azimuth x Range).
DEM_BUFFER = 0.2        
AZIMUTH_LOOKS = 7       # Increase to 14 if noise is still high
RANGE_LOOKS = 19        # Increase to 38 if noise is still high

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("InSAR_Pipeline")

def main():
    logger.info("Starting Integrated Sentinel-1 InSAR Pipeline...")

    # ==========================================================================
    # 2. Initialization
    # ==========================================================================
    # Initialize manager instances with the working directory
    slc_man = SLCManager(work_dir=WORK_DIR)
    orb_man = OrbitManager(work_dir=WORK_DIR)
    dem_man = DEMManager(work_dir=WORK_DIR)
    processor = ISCEProcessor(work_dir=WORK_DIR)

    # ==========================================================================
    # 3. SLC Acquisition (Master & Secondary)
    # ==========================================================================
    logger.info("[Step 1] Searching & Downloading SLCs...")
    
    # Search for available scenes based on ROI and date range
    scenes = slc_man.search(roi_wkt=ROI_WKT, start_date=START_DATE, end_date=END_DATE)
    
    # Validate scene count (Minimum 2 scenes required for InSAR)
    if len(scenes) < 2:
        logger.error("Not enough scenes found for Interferometry. Exiting pipeline.")
        return

    # Download scenes and parse metadata
    slc_paths = slc_man.download_scenes(scenes)
    st_slc = slc_man.get_status() # Retrieve Master/Secondary metadata

    # ==========================================================================
    # 4. Orbit Retrieval
    # ==========================================================================
    logger.info("[Step 2] Fetching Precise Orbits...")
    
    # Automatically fetch POEORB (Precise) or RESORB (Restituted) files
    orbit_paths = orb_man.fetch_orbits(slc_paths)
    st_orb = orb_man.get_status()

    # ==========================================================================
    # 5. DEM Preparation (The "Large Buffer" Strategy)
    # ==========================================================================
    logger.info(f"[Step 3] Preparing DEM with buffer: {DEM_BUFFER} deg...")
    
    # CRITICAL STRATEGY:
    # Pass 'slc_manager' instead of 'roi_wkt' to ensure the DEM covers the 
    # ENTIRE intersection of the SLCs, not just the small ROI.
    # This prevents "east/west limit insufficient" errors during 'runTopo'.
    dem_path = dem_man.prepare_dem(
        slc_manager=slc_man, 
        dem_name='glo_30',
        buffer_deg=DEM_BUFFER, 
        overwrite=False  # Reuse existing DEM if available
    )
    st_dem = dem_man.get_status()

    # ==========================================================================
    # 6. ISCE Configuration (The "Small Processing" Strategy)
    # ==========================================================================
    logger.info("[Step 4] Generating topsApp.xml Configuration...")
    
    # CRITICAL STRATEGY:
    # Although we have a large DEM, we pass 'roi_wkt=ROI_WKT' to ISCE.
    # This instructs 'topsApp.py' to ONLY process the bursts overlapping with the ROI.
    # Result: Unnecessary Swaths (e.g., IW3) are skipped, saving time and reducing errors.
    processor.create_config(
        slc_status=st_slc,
        orbit_status=st_orb,
        dem_status=st_dem,
        roi_wkt=ROI_WKT,        # Explicitly restrict processing to ROI
        slc_bbox=None,          # ROI is provided, so BBox is not needed
        unwrapper="snaphu",     # Phase unwrapping algorithm
        azimuth_looks=AZIMUTH_LOOKS,
        range_looks=RANGE_LOOKS
    )

    # ==========================================================================
    # 7. Execution
    # ==========================================================================
    logger.info("[Step 5] Running ISCE topsApp.py...")
    
    try:
        # Execute the ISCE pipeline
        processor.run_process()
        logger.info("ISCE Processing Completed Successfully.")
    except Exception as e:
        logger.error(f"Processing Failed: {e}")
        return

    # ==========================================================================
    # 8. Result Check (Basic Validation)
    # ==========================================================================
    logger.info("[Step 6] Validating Results...")
    
    # Check for geocoded output files
    results = processor.get_results(geocoded_only=True)
    
    if not results.empty:
        logger.info(f"   Found {len(results)} output files.")
        target_file = "merged/filt_topophase.unw.geo"
        
        # Verify the existence of the final unwrapped phase file
        full_path = os.path.join(processor.process_dir, target_file)
        if os.path.exists(full_path):
            logger.info(f"Final Output Found: {full_path}")
            logger.info("   -> Ready for Post-Processing (Reference Point Correction).")
        else:
            logger.warning(f"Pipeline finished, but target file {target_file} is missing.")
    else:
        logger.error("No output files found. Something went wrong during processing.")

if __name__ == "__main__":
    main()