from datetime import date, timedelta

start_date = date(2019, 1, 1)
end_date = date(2020, 1, 1)
lst = ["00:00:00", "12:00:00"]
delta = timedelta(days=1)
while start_date <= end_date:
    for t in lst:
        print(start_date.strftime("%Y-%m-%d"))
        day = start_date.strftime("%Y-%m-%d")
        server.retrieve({
            "class": "ti",
            "dataset": "tigge",
            "date": day,
            "expver": "prod",
            "grid": "0.5/0.5",
            "levtype": "sfc",
            "origin": "ecmf",
            "param": "147/228039",
            "step": "0/12",
            "time": t,
            "type": "cf",
            "target": day+"-"+t.replace(":","")+".grib",
            "area": "10/-130/30/-60",
        })
    start_date += delta
