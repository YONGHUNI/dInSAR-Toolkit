import os
import tempfile
import hashlib
from pathlib import Path
import shapely.wkt
from shapely.geometry import box, shape
from shapely.ops import unary_union
import rasterio
from dem_stitcher.stitcher import stitch_dem
from osgeo import gdal
from modules.auth_base import EarthdataAuth

class DEMManager(EarthdataAuth):
    """
    Manages Digital Elevation Model (DEM) acquisition and preparation for ISCE processing.

    This class automates the workflow of downloading, stitching, and converting DEM data.
    It leverages 'dem_stitcher' for data retrieval with precise Geoid-to-Ellipsoid correction
    and uses GDAL to convert the result into the ISCE-compatible format (.wgs84).
    
    It separates intermediate byproducts (GeoTIFF) from the final output to maintain a clean workspace.

    Attributes:
        save_dir (Path): Directory where the final ISCE product (.wgs84) will be stored.
        temp_dir (Path): Directory for intermediate files (e.g., stitched GeoTIFFs).
        current_tif (Path): Path to the currently processed (stitched) GeoTIFF file.
        dem_file (Path): Path to the final ISCE-compatible DEM file.
    """

    def __init__(self, save_dir="dem_data", temp_dir=None):
        """
        Initializes the DEMManager.

        Args:
            save_dir (str): Directory path to save the final ISCE DEM. Defaults to "dem_data".
            temp_dir (str, optional): Directory path for intermediate byproducts (GeoTIFF).
                                      If None, the system's default temporary directory is used.
        """
        super().__init__()
        
        self.save_dir = Path(save_dir)
        self.save_dir.mkdir(parents=True, exist_ok=True)
        
        if temp_dir is None:
            self.temp_dir = Path(tempfile.gettempdir())
        else:
            self.temp_dir = Path(temp_dir)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            
        self.current_tif = None
        self.dem_file = None

    def get_status(self):
        """
        Returns the current status and configuration of the manager.

        This method allows external processors (like ISCEProcessor) to retrieve
        the DEM file path and directory information without accessing internal attributes.

        Returns:
            dict: A dictionary containing module status, save/temp directories, and the final DEM path.
        """
        return {
            "module_name": "DEMManager",
            "is_ready": self.dem_file is not None and self.dem_file.exists(),
            "save_dir": str(self.save_dir.resolve()),
            "temp_dir": str(self.temp_dir.resolve()),
            "dem_path": str(self.dem_file.resolve()) if self.dem_file else None
        }

    # --------------------------------------------------------------------------
    # 1. Main Logic: Fetch & Export
    # --------------------------------------------------------------------------
    def fetch_dem(self, slc_manager=None, roi_wkt=None, dem_name='glo_30', buffer_deg=0.1, overwrite=False):
        """
        Calculates the target bounds and downloads the stitched DEM (GeoTIFF) to the temp directory.

        Priority:
            1. If `slc_manager` is provided, calculates the intersection of the Master and all Slaves.
            2. If `roi_wkt` is provided, uses the bounds of that polygon.

        Args:
            slc_manager (S1SLCManager, optional): Instance of SLCManager with Master/Slaves set.
            roi_wkt (str, optional): WKT string defining the Region of Interest.
            dem_name (str): The DEM dataset name ('glo_30', 'nasadem', etc.). Defaults to 'glo_30'.
            buffer_deg (float): Buffer to add around the bounds in degrees. Defaults to 0.1.

        Returns:
            Path or None: Path to the downloaded intermediate GeoTIFF file. Returns None on failure.
        """
        # 1. Calculate Bounds
        bounds = None
        if slc_manager is not None:
            print("[DEMManager] Calculating bounds from SLC intersection...")
            bounds = self._calculate_intersection_bounds(slc_manager)
            if bounds is None:
                print("[Error] Could not calculate intersection from SLCManager.")
                return None
        elif roi_wkt is not None:
            print("[DEMManager] Using provided ROI WKT...")
            bounds = shapely.wkt.loads(roi_wkt).bounds
        else:
            print("[Error] Provide 'slc_manager' or 'roi_wkt'.")
            return None

        # 2. Apply Buffer
        bounds = self._add_buffer(bounds, buffer_deg)
        
        # 3. Generate Unique Hash for this specific request
        # Includes bounds, dem_name, and buffer to ensure uniqueness
        hash_input = f"{bounds}_{dem_name}_{buffer_deg}"
        request_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:12] # Use first 12 chars
        
        tif_filename = f"dem_{dem_name}_{request_hash}.tif"
        output_path = self.temp_dir / tif_filename
        
        print(f"   Target Bounds (SNWE): {bounds[1]:.4f}, {bounds[3]:.4f}, {bounds[0]:.4f}, {bounds[2]:.4f}")
        print(f"   Cache Hash: {request_hash} -> File: {tif_filename}")

        # 4. Check for existing file (Caching)
        if not overwrite and output_path.exists() and output_path.stat().st_size > 1024:
            print(f"   [Info] Matching DEM cache found. Skipping download.")
            self.current_tif = output_path
            return self.current_tif

        # 5. Stitch to GeoTIFF (Actual Download)
        if self._stitch_geotiff(bounds, dem_name, output_path):
            self.current_tif = output_path
            return self.current_tif
        
        return None

    def export_to_isce(self, overwrite=False):
        """
        Converts the fetched intermediate GeoTIFF to the ISCE-compatible format (.wgs84).

        Args:
            overwrite (bool): If True, forces regeneration of the file even if it exists.
                              If False, skips conversion if a valid ISCE file is already found.

        Returns:
            Path or None: Path to the final ISCE DEM file. Returns None on failure.
        """
        if not self.current_tif or not self.current_tif.exists():
            print("[Error] No fetched DEM found. Run 'fetch_dem()' first.")
            return None

        isce_path = self.save_dir / "dem.wgs84"
        print(f"[DEMManager] Exporting to ISCE format: {isce_path}")

        # Check validity of existing ISCE file
        if not overwrite and self._is_valid_isce_file(isce_path):
             print(f"   [Info] Valid ISCE file already exists. Skipping conversion.")
             self.dem_file = isce_path
             return self.dem_file

        if self._convert_to_isce(self.current_tif, isce_path):
            self.dem_file = isce_path
            print(f"   ✅ Export complete.")
            return self.dem_file
        return None

    def prepare_dem(self, overwrite=False, **kwargs):
        """
        Automated wrapper: Executes fetch_dem() followed by export_to_isce().

        Args:
            overwrite (bool): Whether to overwrite the existing ISCE file.
            **kwargs: Arguments passed to fetch_dem() (e.g., slc_manager, roi_wkt).

        Returns:
            Path or None: Path to the final DEM file.
        """
        if self.fetch_dem(overwrite=overwrite, **kwargs):
            return self.export_to_isce(overwrite=overwrite)
        return None

    def plot_dem(self, roi_wkt=None, max_pixels=800):
        """
        Visualizes the coverage of the downloaded DEM and the actual terrain on an interactive map.

        [Performance Optimization]
        To prevent browser lag when rendering large areas, the DEM image is automatically 
        downsampled to fit within 'max_pixels' dimensions for visualization purposes. 
        (The actual physical data file remains in its original high resolution.)

        Args:
            roi_wkt (str, optional): Original ROI WKT to overlay for comparison.
            max_pixels (int): Maximum width/height in pixels for the visualization image.
                              Defaults to 800. Larger images will be scaled down.

        Returns:
            folium.Map: Interactive map object. Returns None if no DEM is fetched.
        """
        if not self.current_tif or not self.current_tif.exists():
            print("[Warning] No DEM fetched yet. Run 'fetch_dem()' first.")
            return None

        import numpy as np
        import matplotlib.pyplot as plt
        import folium
        from rasterio.enums import Resampling

        # Read GeoTIFF (With Downsampling)
        with rasterio.open(self.current_tif) as src:
            b = src.bounds
            image_bounds = [[b.bottom, b.left], [b.top, b.right]]
            
            # 1. Calculate Scale Factor
            scale = min(1.0, max_pixels / max(src.width, src.height))
            
            # 2. Conditional Read
            if scale < 1.0:
                new_width = int(src.width * scale)
                new_height = int(src.height * scale)
                
                # Resample while reading to save memory
                data = src.read(
                    1,
                    out_shape=(new_height, new_width),
                    resampling=Resampling.bilinear
                )
            else:
                data = src.read(1)

            nodata = src.nodata
            if nodata is not None:
                data = np.ma.masked_equal(data, nodata)
            
            # Normalize and Colorize
            d_min, d_max = np.nanmin(data), np.nanmax(data)
            if d_max > d_min:
                norm_data = (data - d_min) / (d_max - d_min)
            else:
                norm_data = np.zeros_like(data, dtype=float)
            
            cmap = plt.get_cmap('terrain')
            colored_data = cmap(norm_data)
            img_array = (colored_data * 255).astype(np.uint8)

        # Create Map
        center_lat = (b.bottom + b.top) / 2
        center_lon = (b.left + b.right) / 2
        m = folium.Map(location=[center_lat, center_lon], zoom_start=11)

        # 1. DEM Layer
        folium.raster_layers.ImageOverlay(
            image=img_array, 
            bounds=image_bounds, 
            opacity=0.6, 
            name="Elevation (Preview)", 
            mercator_project=True
        ).add_to(m)

        # 2. DEM Boundary Box (Red Dashed)
        folium.GeoJson(
            data=shapely.geometry.mapping(box(b.left, b.bottom, b.right, b.top)),
            style_function=lambda x: {'color': 'black', 'fill': False, 'weight': 1},
            name="DEM Boundary"
        ).add_to(m)

        # 3. Requested ROI (Red Dashed Line - Visibility Improved)
        if roi_wkt:
            folium.GeoJson(
                shapely.wkt.loads(roi_wkt), 
                style_function=lambda x: {
                    'color': 'red',       # 빨간색
                    'weight': 3,          # 두께 3
                    'fill': False,        # 채우기 없음 (배경 보이게)
                    'dashArray': '5, 5'   # 점선 효과
                },
                tooltip="Requested ROI",
                name="Requested ROI"
            ).add_to(m)

        folium.LayerControl().add_to(m)
        return m

    # --------------------------------------------------------------------------
    # 2. Internal Helpers
    # --------------------------------------------------------------------------
    def _stitch_geotiff(self, bounds, dem_name, output_path):
        """
        Downloads and stitches DEM tiles into a single GeoTIFF using 'dem_stitcher'.
        
        Crucial: Sets 'dst_ellipsoidal_height=True' to convert the vertical datum 
        from Geoid (EGM96) to Ellipsoid (WGS84), which is required for ISCE.
        """
        if output_path.exists() and output_path.stat().st_size > 1024:
            print(f"   [Info] Intermediate GeoTIFF exists: {output_path}")
            return True
        print(f"[DEMManager] Downloading & Stitching '{dem_name}' to {output_path}...")
        try:
            X, p = stitch_dem(
                bounds, dem_name=dem_name, dst_ellipsoidal_height=True, dst_area_or_point='Area'
            )
            with rasterio.open(output_path, 'w', **p) as ds:
                ds.write(X, 1)
            return True
        except Exception as e:
            print(f"[Error] Stitching failed: {e}")
            return False

    def _convert_to_isce(self, input_tif, output_isce):
        """Converts a GeoTIFF file to the ISCE binary format using GDAL."""
        try:
            gdal.Translate(str(output_isce), str(input_tif), format="ISCE")
            return True
        except Exception as e:
            print(f"[Error] ISCE Conversion failed: {e}")
            return False

    def _is_valid_isce_file(self, path):
        """
        Performs a lightweight integrity check on the ISCE file.
        Checks for: file existence, non-zero size, XML metadata, and valid GDAL header.
        """
        path = Path(path)
        if not path.exists() or path.stat().st_size < 1024: return False
        if not path.with_suffix(".wgs84.xml").exists(): return False
        try:
            ds = gdal.Open(str(path), gdal.GA_ReadOnly)
            if ds is None: return False
            ds = None
            return True
        except: return False

    def _add_buffer(self, bounds, buffer):
        """Adds a buffer (margin) to the bounding box coordinates."""
        minx, miny, maxx, maxy = bounds
        return (minx - buffer, miny - buffer, maxx + buffer, maxy + buffer)

    def _calculate_intersection_bounds(self, slc_manager):
        """
        Calculates the geographic intersection of the Master scene and all selected Slave scenes.
        
        Compatible with both 'asf_search' result objects (method access) and 
        Mock objects (dictionary access) for testing flexibility.
        """
        if slc_manager.master_idx is None:
            return None
        
        # Helper: Safely extract geometry regardless of object type
        def get_geometry(scene):
            # Case 1: asf_search object (geojson is a method)
            if hasattr(scene, 'geojson') and callable(scene.geojson):
                return shape(scene.geojson()['geometry'])
            
            # Case 2: Mock object (geojson is a dict/property)
            if hasattr(scene, 'geojson') and isinstance(scene.geojson, dict):
                return shape(scene.geojson['geometry'])
            
            # Case 3: Fallback (Try accessing property if it exists)
            if hasattr(scene, 'geometry'): 
                return shape(scene.geometry)

            return None

        # 1. Master Geometry
        master_geom = get_geometry(slc_manager.master_scene)
        if master_geom is None:
            print("[Error] Master scene has no valid geometry.")
            return None

        # 2. Slave Geometries
        slave_indices = slc_manager.selected_indices
        if not slave_indices:
            return master_geom.bounds

        slave_geoms = []
        for idx in slave_indices:
            if idx == slc_manager.master_idx: continue
            
            s_geom = get_geometry(slc_manager.results[idx])
            if s_geom:
                slave_geoms.append(s_geom)

        # 3. Intersection Calculation
        if not slave_geoms:
            return master_geom.bounds

        slaves_union = unary_union(slave_geoms)
        common_area = master_geom.intersection(slaves_union)
        
        return common_area.bounds if not common_area.is_empty else None