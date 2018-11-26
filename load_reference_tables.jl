using DataFrames
using CSV
using RCall
using MySQL


function df2mysql(df, stmt)
    for i in 1:size(df, 1)
        d = vec(convert(Array, df[i,:]))
        MySQL.execute!(stmt, d)
    end
end


# ------------------ create database
conn = MySQL.connect("127.0.0.1", "root", "")
# MySQL.execute!(conn, "DROP DATABASE if exists riair")
# MySQL.execute!(conn, "CREATE DATABASE riair")
MySQL.execute!(conn, "USE riair")


# ------------------ drop tables
MySQL.execute!(conn, "DROP TABLE if exists outcome_desc")
MySQL.execute!(conn, "DROP TABLE if exists year_desc")

MySQL.execute!(conn, "DROP TABLE if exists school_polygon")
MySQL.execute!(conn, "DROP TABLE if exists preschool_polygon")
MySQL.execute!(conn, "DROP TABLE if exists address_polygon")
MySQL.execute!(conn, "DROP TABLE if exists pcp_polygon")
MySQL.execute!(conn, "DROP TABLE if exists cohort_year")
MySQL.execute!(conn, "DROP TABLE if exists cohort_polygon")


# ------------------ create tables
MySQL.execute!(conn, """CREATE TABLE outcome_desc
(
    outcome_id INT NOT NULL PRIMARY KEY,
    outcome_desc VARCHAR(255)
);""")
MySQL.execute!(conn, """CREATE TABLE year_desc
(
    year_id INT NOT NULL PRIMARY KEY,
    year_desc VARCHAR(255)
);""")

MySQL.execute!(conn, """CREATE TABLE school_polygon
(
    cohort_id INT,
    school_name VARCHAR(255) NOT NULL,
    polygon_id INT,
    city VARCHAR(255),
    PRIMARY KEY (school_name, city)
);""")
MySQL.execute!(conn, """CREATE TABLE preschool_polygon
(
    cohort_id INT,
    school VARCHAR(255) NOT NULL,
    polygon_id INT,
    city VARCHAR(255) NOT NULL,
    PRIMARY KEY (school, city)
);""")
MySQL.execute!(conn, """CREATE TABLE address_polygon
(
    fid INT NOT NULL PRIMARY KEY,
    zip INT,
    city VARCHAR(255),
    home_address VARCHAR(255),
    polygon_id INT -- no foreign key because polygon_id can be 0 in this table
);""")
MySQL.execute!(conn, """CREATE TABLE pcp_polygon
(
    pcp_id INT NOT NULL AUTO_INCREMENT,
    cohort_id INT,
    pcp_name VARCHAR(255) NOT NULL,
    pcp_address VARCHAR(255) NOT NULL,
    polygon_id INT,
    city VARCHAR(255),
    PRIMARY KEY (pcp_id)
);""")
MySQL.execute!(conn, """CREATE TABLE cohort_year
(
    cohort_id INT NOT NULL,
    year_id INT NOT NULL,
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (cohort_id, year_id)
);""")
MySQL.execute!(conn, """CREATE TABLE cohort_polygon
(
    cohort_id INT,
    polygon_id INT NOT NULL PRIMARY KEY
);""")


# ------------------ mapping files
R"address_polygon = read.csv('~/repos/riair/data/E911_Polygon_Table.csv', na.strings = c('', ' '))"
R"address_polygon[] <- lapply(address_polygon, function(x) if(is.factor(x)) as.character(x) else x)"
@rget address_polygon
address_polygon[:PrimaryAdd] = lowercase.(strip.(address_polygon[:PrimaryAdd]))
address_polygon[:ZN]= lowercase.(strip.(address_polygon[:ZN]))
address_polygon = address_polygon[:, [:FID, :Zip, :ZN, :PrimaryAdd, :Poly_ID]]
school_polygon = readtable("./data/RIAirSchoolPolygon.csv")
preschool_polygon = readtable("./data/RIAirPreschoolPolygon.csv")
pcp_polygon = readtable("./data/RIAirProvidersPolygon.csv")
pcp_polygon[:Name_of_provider] = lstrip.(pcp_polygon[:Name_of_provider])
pcp_polygon[:Name_of_provider] = rstrip.(pcp_polygon[:Name_of_provider])
year_desc = readtable("./data/year_desc.csv")
cohort_year = readtable("./data/cohort_year.csv")
cohort_polygon = readtable("./data/cohort_polygon.csv")
outcome_desc = DataFrame(outcome_id = collect(1:4),
                         outcome_desc = ["Eligible", "Ineligible", "Ineligible - holding pattern",
                                         "Ineligible - other community asthma programs"])


# ------------------ insert dataframes into mysql tables
stmt = MySQL.Stmt(conn, "INSERT INTO outcome_desc (outcome_id, outcome_desc) VALUES (?,?);")
df2mysql(outcome_desc, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO year_desc (year_id, year_desc) VALUES (?,?);")
df2mysql(year_desc, stmt)

stmt = MySQL.Stmt(conn, "INSERT INTO cohort_polygon (cohort_id, polygon_id) VALUES (?,?);")
df2mysql(cohort_polygon, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO school_polygon (cohort_id, school_name, polygon_id, city) VALUES (?,?,?,?);")
df2mysql(school_polygon, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO preschool_polygon (cohort_id, school, polygon_id, city) VALUES (?,?,?,?);")
df2mysql(preschool_polygon, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO address_polygon(fid, zip, city, home_address, polygon_id) VALUES (?,?,?,?,?);")
df2mysql(address_polygon, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO pcp_polygon (cohort_id, pcp_name, pcp_address, polygon_id, city) VALUES (?,?,?,?,?);")
df2mysql(pcp_polygon, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO cohort_year (cohort_id, year_id, start_date, end_date) VALUES (?,?,?,?);")
df2mysql(cohort_year, stmt)
