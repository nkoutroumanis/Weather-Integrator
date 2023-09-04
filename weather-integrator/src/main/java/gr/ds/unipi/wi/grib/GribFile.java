package gr.ds.unipi.wi.grib;

import java.util.List;

public interface GribFile {

    static double roundToHalf(double f) {
        return Math.round(f * 2) / 2.0;
    }

    static int getLatIndex(double fLat) { // lats: 90...-90 per 0.5 (361 values)
        double dLat = roundToHalf(fLat);
        int i = (int) (2 * (90 - dLat));
        return i;
    }

    static int getLonIndex(double fLon) { // lons: 0...359.5, per 0.5  (720 values)
        double dLon = roundToHalf(fLon);
        int i = (int) (2 * dLon);
        if (i < 0) {
            i = 720 + i;
        }
        return i;
    }

    static double toRad(double value) {
        return value * Math.PI / 180;
    }

    static double haversine(double lon1, double lat1, double lon2, double lat2){
        final int R = 6371; // Radious of the earth
        double latDistance = toRad(lat2-lat1);
        double lonDistance = toRad(lon2-lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
                        Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }


    List<Object> getDataValuesByLatLon(double lat, double lon);

    List<Object> getDataValuesByLatLonInterpolated(double lat, double lon);

}
