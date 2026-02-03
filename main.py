from pathlib import Path
import argparse

from modules.SLC_manager import S1SLCManager
from modules.orbit_manager import OrbitManager
from modules.DEM_manager import DEMManager
from modules.isce_processor import ISCEProcessor


def main():
    parser = argparse.ArgumentParser(description="dInSAR Toolkit main runner")
    parser.add_argument("--roi", nargs=4, type=float, required=True,
                        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"))
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--work_dir", default="work")
    parser.add_argument("--download_dir", default="data/slc")
    parser.add_argument("--orbit_dir", default="data/orbits")
    parser.add_argument("--dem_dir", default="data/dem")
    parser.add_argument("--project", default="insar_project")

    args = parser.parse_args()

    roi = tuple(args.roi)

    work_dir = Path(args.work_dir).resolve()
    download_dir = Path(args.download_dir).resolve()
    orbit_dir = Path(args.orbit_dir).resolve()
    dem_dir = Path(args.dem_dir).resolve()

    for d in [work_dir, download_dir, orbit_dir, dem_dir]:
        d.mkdir(parents=True, exist_ok=True)

    print("=== SLC search ===")
    slc = S1SLCManager(download_dir=download_dir)
    slc.search_images(
        roi=roi,
        start_date=args.start,
        end_date=args.end,
    )

    results = slc.view_selected()
    if results is None or len(results) < 2:
        raise RuntimeError("At least 2 SLC scenes are required.")

    # 간단 정책: 첫 장면 master, 두 번째 slave
    master_id = results.iloc[0]["scene_id"]
    slave_id = results.iloc[1]["scene_id"]

    slc.set_master(master_id)
    print(f"Master set to: {master_id}")

    print("=== Download SLC ===")
    slc.download_selected()

    print("=== Orbit fetch ===")
    orb = OrbitManager(orbit_dir=orbit_dir)
    orb.fetch_orbits(slc_manager=slc, precise_only=True)

    print("=== DEM prepare ===")
    dem = DEMManager(save_dir=dem_dir)
    dem_path = dem.prepare_dem(
        slc_manager=slc,
        buffer_deg=0.2
    )
    print(f"DEM ready: {dem_path}")

    print("=== ISCE processing ===")
    proc = ISCEProcessor(
        work_dir=work_dir,
        raw_data_dir=download_dir,
        dem_path=dem_path
    )

    config_path = proc.create_config(
        project_name=args.project,
        master_id=master_id,
        slave_ids=[slave_id]
    )

    print(f"topsApp config: {config_path}")
    proc.run_process()

    print("=== Collect results ===")
    results = proc.get_results()
    for k, v in results.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
