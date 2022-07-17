import re
import pandas as pd

def dms2dec(dms_str):
    """dms2dec."""
    print dms_str
    try:
        dms_str = re.sub(r'\s', ' ', dms_str)
        # print dms_str
        # sign = -1 if re.search('[swSW]', dms_str) else 1

        numbers = filter(len, re.split('\D+', dms_str, maxsplit=4))
        # print numbers
        degree = numbers[0]
        minute = numbers[1] if len(numbers) >= 2 else '0'
        second = numbers[2] if len(numbers) >= 3 else '0'
        frac_seconds = numbers[3] if len(numbers) >= 4 else '0'
        second += "." + frac_seconds
        return (int(degree) + float(minute) / 60 + float(second) / 3600)
    except Exception:
        return None

# # print dms2dec(None)

# latlong = pd.read_excel('/Users/biswadippaul/Projects/GETCO/WIND NAME NEW2.xlsx',
#                         sheetname='LatLong')
# # print latlong

# latlong['Latitude1'] = latlong.apply(lambda row: dms2dec(row['Latitude']), axis=1)
# latlong['Longitude1'] = latlong.apply(lambda row: dms2dec(row['Longitude']), axis=1)

# latlong[['Latitude1', 'Longitude1']].to_csv('/Users/biswadippaul/Downloads/latlong.csv')