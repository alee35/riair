address_polygon = read.csv("~/repos/riair/data/E911_Polygon_Table.csv", na.strings = c("", " "))
write.csv(address_polygon, "~/repos/riair/data/RIAirAddressPolygon.csv", row.names = FALSE, na="")