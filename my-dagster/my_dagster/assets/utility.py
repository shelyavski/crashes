import numpy as np
from time import strptime


def get_hour_minute(t_str: str) -> tuple[int, int]:
    # Cases: 10:05, 10:05A, 10:05AM, 10:0 A
    t_format = '%H:%M'

    if t_str.endswith(('A', 'P')):
        t_str += 'M'

    if t_str.endswith(('AM', 'PM')):
        t_format += '%p'

    time_struct = strptime(t_str, t_format)
    return time_struct.tm_hour, time_struct.tm_min


def reformat_time_string(small_t_str: str) -> tuple[str, str]:
    only_num = [char for char in small_t_str if char.isnumeric()]
    non_num = ''.join([char for char in small_t_str if not char.isnumeric()])

    if len(only_num) == 1:
        only_num.insert(0, '0')

    only_num = ''.join(only_num)
    return only_num, non_num


def get_reformatted_hour_minute(t_string: str) -> tuple[int, int] | np.NaN:
    hour_str, minute_str = t_string.split(':')

    new_hour = int(reformat_time_string(hour_str)[0])
    new_minute = int(reformat_time_string(minute_str)[0])
    morning_indicator = reformat_time_string(minute_str)[1]

    # Sometimes the data looks like this 38:76PM
    # This is unfixable. Better to return NaN
    fail_conditions = [
        new_hour > 23 and new_minute > 23,
        new_hour > 60 or new_minute > 60
    ]

    if any(fail_conditions):
        return np.NaN

    # Sometimes the hour and minute are switched
    if new_hour > 23 >= new_minute:
        new_hour, new_minute = new_minute, new_hour

    reformatted_str = f"{new_hour}:{new_minute}{morning_indicator}"

    return get_hour_minute(reformatted_str)
