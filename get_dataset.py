import cdsapi
from os import listdir, getcwd
from os.path import isfile, join
import xarray as xr


# get files in folder
onlyfiles = [f for f in listdir(getcwd()) if isfile(join(getcwd(), f))]


def get_dataset_sample(year: int) -> str | None:
    # dataset name
    dataset = "reanalysis-era5-single-levels"
    # plain request for api
    request = {
        "product_type": ["reanalysis"],
        "year": [year],
        "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ],
        "day": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12",
            "13", "14", "15",
            "16", "17", "18",
            "19", "20", "21",
            "22", "23", "24",
            "25", "26", "27",
            "28", "29", "30",
            "31"
        ],
        "time": [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "variable": ["2m_temperature"],
        "area": [80, 20, 40, 80]
    }
    # target filename
    target = f'{str(year)}.nc'

    # download only if file doesn't exist
    if target not in onlyfiles:
        print(f"Missing file for {year} year, downloading...")
        # request to api
        client = cdsapi.Client()
        client.retrieve(dataset, request, target)

    return target


def get_whole_dataset(as_one_file: bool, year_range: list) -> str | list:
    # get files of dataset sample by years
    filename_list = []
    for year in year_range:
        filename = get_dataset_sample(year)
        filename_list.append(filename)
    print("All files for dataset have been downloaded")

    if as_one_file:
        if "t2m-dataset.nc" not in onlyfiles:
            print("Computing whole dataset as one file...")
            # concat all datasets into one dataset
            with xr.open_mfdataset(
                    paths=filename_list,
                    concat_dim="valid_time",
                    combine="nested"
            ) as ds:
                # save
                ds.to_netcdf("t2m-dataset.nc", compute=True)
        print("Dataset files have been parsed into one file 't2m-dataset.nc'")

    return "t2m-dataset.nc" if as_one_file else filename_list
