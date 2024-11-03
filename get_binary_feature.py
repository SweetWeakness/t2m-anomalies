import numpy as np
import xarray as xr

from get_dataset import get_whole_dataset


def calc_bin_target(dataset):
    print("Started to calculate binary target for task...")
    # todo dayofyear -> date
    # grouping by days and calculate "t2m" mean
    ds_new = dataset.groupby("valid_time.dayofyear").mean()
    # then calculate delta of "t2m" between days
    ds_new = ds_new.diff("dayofyear")
    # calculate abs for delta
    ds_new = ds_new.map(np.fabs)
    # then calculate binary target (task)
    ds_new = ds_new.map(lambda x: np.where(x >= 5, 1, 0))
    print("Target calculated, saving...")
    # save
    ds_new.to_netcdf("task.nc")


def get_analyzed_dataset(as_one_file: bool, year_range: list):
    # get dataset
    filename = get_whole_dataset(as_one_file=as_one_file,
                                 year_range=year_range)

    if as_one_file:
        with xr.open_dataset(filename,
                             engine="netcdf4") as ds:
            calc_bin_target(ds)

    else:
        with xr.open_mfdataset(paths=filename,
                               concat_dim="valid_time",
                               combine="nested"
                               ) as ds:
            calc_bin_target(ds)


# Вот тут решение для 2010 - 2020 по РФ
# get_analyzed_dataset(as_one_file=True, year_range=list(range(2010, 2021)))

# Вот решение для года
year = 2010
filename = f"{year}.nc"
with xr.open_dataset(filename, engine="netcdf4") as ds:
    calc_bin_target(ds)
