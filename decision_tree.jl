
using DataFrames
using CSV
using RCall
using Combinatorics
using DataStreams
using MySQL

# ------------------ mapping files

R"address_polygon = read.csv('~/repos/riair/data/E911_Polygon_Table.csv', na.strings = c('', ' '))"
@rget address_polygon
# CSV.write("./data/RIAirAddressPolygonJL.csv", address_polygon)
# address_polygon_test = CSV.read("./data/RIAirAddressPolygonJL.csv")
school_polygon = readtable("./data/RIAirSchoolPolygon.csv")
preschool_polygon = readtable("./data/RIAirPreschoolPolygon.csv")
pcp_polygon = readtable("./data/RIAirProvidersPolygon.csv")
pcp_polygon[:Name_of_provider] = lstrip.(pcp_polygon[:Name_of_provider])
pcp_polygon[:Name_of_provider] = rstrip.(pcp_polygon[:Name_of_provider])
year_desc = readtable("./data/year_desc.csv")
cohort_year = readtable("./data/cohort_year.csv")
cohort_polygon = readtable("./data/cohort_polygon.csv")

# ------------------ dummy data

id = [1,1, 2,3,4,5,6,7,8,9,10,11, 12,12,12]
school = ["Veazie Street Elementary School","Veazie Street Elementary School","Fake School",
"Pleasant View Elementary School","Fake School","Times Academy","Fake Academy",
"George J. West Elementary School","Fake School","Alfred Lima Sr. Elementary School",
"Fake School","Henry J. Winters Elementary School","Fake School","Fake School","Fake School"]
address = ["21 FAKE AVE","21 FAKE AVE","0 ABBOTT ST","3 FAKE ST","0 ABBOTT ST",
"14 FAKE ST","0 ABBOTT ST","118 FAKE AVE","15 FLORAL PARK BLVD","2 FAKE ST",
"15 FLORAL PARK BLVD","138 FAKE AVE","134 FAKE AVE","134 FAKE AVE","134 FAKE AVE"]
city = ["FAKE CITY", "FAKE CITY", "PROVIDENCE", "FAKE CITY", "PROVIDENCE", "FAKE CITY", "PAWTUCKET",
"FAKE CITY", "PAWTUCKET", "FAKE CITY","PROVIDENCE", "FAKE CITY", "FAKE CITY","FAKE CITY","FAKE CITY"]
pcp = ["Dr. Ellen L. Gurney, Pediatrician","Dr. Ellen L. Gurney, Pediatrician","No Name",
"Dr. Ellen L. Gurney, Pediatrician","No Name","Dr. Ellen L. Gurney, Pediatrician",
"No Name","Dr. Ellen L. Gurney, Pediatrician", "No Name","Dr. Ellen L. Gurney, Pediatrician",
"No Name","No Name","Dr. Ellen L. Gurney, Pediatrician","Dr. Ellen L. Gurney, Pediatrician",
"Dr. Chad P. Nevola, Pediatrician"]
age =              [1,1,1,1,1, 9,9,9,9, 15,15,15,15,15,15]
parent_dx =        [0,0,0,1,1, 0,0,1,1,  0, 0, 1, 1, 1, 1]
prev_participant = [0,0,1,0,1, 0,1,0,1,  0, 1, 0, 1, 0, 1]
dob = ["2017-07-01","2017-07-01","2017-07-01","2017-07-01","2017-07-01", 
       "2009-07-01","2009-07-01","2009-07-01","2009-07-01", 
       "2004-07-01","2004-07-01","2004-07-01","2015-07-01","2015-07-01","2015-07-01"]
encounter_date = ["2017-01-01","2017-03-01","2018-04-07", "2018-12-01",
                  "2018-04-07","2019-12-01","2018-04-07", "2020-07-01",
                  "2018-04-07", "2018-12-01", "2018-04-07","2018-04-07",
                  "2017-12-01","2018-12-01","2020-12-01"]
df = DataFrame(id = id, 
              dob=dob,
              address = address,
              city = city,
              pcp = pcp,
              parent_dx = parent_dx,
              prev_participant = prev_participant,
              encounter_date = encounter_date
              )

# CSV.write("./data/test.csv", df)

# ------------------ generate all combinations of test data
# dob = ["01-15-2016", "01-15-2008", "01-15-2002"]
# encounter_date = ["01-15-2017", "01-15-2018", "01-15-2019", "01-15-2020",
#                   "01-15-2021", "01-15-2022", "01-15-2023"]
# address = ["0 ABBOTT ST","15 FLORAL PARK BLVD","21 FAKE AVE"]
# city = ["PROVIDENCE", "PAWTUCKET", "FAKE CITY"]
# pcp = ["Dr. Ellen L. Gurney, Pediatrician", "Dr. Chad P. Nevola, Pediatrician", "Fake Name"]
# parent_dx = [0,1]
# prev_participant = [0,1]
# d = vec(collect(Base.product(parent_dx, prev_participant, encounter_date, address, city, pcp, dob)))
# df = DataFrame(parent_dx=Int64[], prev_participant=Int64[], encounter_date=String[], 
#                address=String[], city=String[], pcp=String[], dob=String[])
# for tup in d
#     push!(df, [t for t in tup])
# end 

# ------------------ utility functions

function in_address_polygon(address, city)
    poly_id = unique(address_polygon[(address_polygon[:PrimaryAdd] .== address) & (address_polygon[:ZN] .== city), :Poly_ID])
    if length(poly_id) > 0
        push!(polygon_id, poly_id[1])
        println("address in polygon")
    elseif length(poly_id) == 0
        println("address not in polygon")
    end
end

function in_pcp_polygon(name)
    poly_id = unique(pcp_polygon[pcp_polygon[:Name_of_provider] .== name, :Polygon_number])
    if name in pcp_polygon[:Name_of_provider]
        push!(polygon_id, poly_id[1])
        println("pcp in polygon")
    elseif (name in pcp_polygon[:Name_of_provider]) == false
        push!(polygon_id, missing)
        println("pcp not in polygon")
    end
end

function compute_age_at_start(dob, das)
    if ismissing(das) == false
        push!(age_at_start, floor(convert(Int64, das - dob) / 365))
        println("in eligible polygon - computing age")
    else
        push!(age_at_start, missing)
        println("no intervention date - not in a polygon")
    end
end

function eligibility(age)
    if ismissing(age) == false
        if age >= 2 && age <= 12
            push!(eligible, 1)
            println("eligible")
        else
        push!(eligible, 0)
        println("ineligible for RI-Air because of patient age. do further checks.")
        end
    else
        push!(eligible, 0)
        println("ineligible for RI-Air because patient not in a polygon. do further checks")
    end
end

function intervention_year(year, start_1, end_1)
    if year >= start_1 && year <= end_1
        return(1)
    elseif year < start_1
        return(0)
    elseif year > end_1
        return(2)
    end
end

function add_row_id(df)
    num_rows = size(df,1)
    old_cols = names(df)
    col_order = vcat([:row_id], old_cols)
    df[:row_id] = collect(1:num_rows)
    df = df[col_order]
end

function df2mysql(df, stmt)
    for i in 1:size(df, 1)
        d = vec(convert(Array, df[i,:]))
        MySQL.execute!(stmt, d)
    end
end

# ------------------ logic

polygon_id = []
age_at_start = []
eligible = []
community_asthma = []
referral = []
outcome = []

# convert dates to Date type
cohort_year[:start_date] = Date.(cohort_year[:start_date], "yyyy-mm-dd")
cohort_year[:end_date] = Date.(cohort_year[:end_date], "yyyy-mm-dd")
df[:dob] = Date.(df[:dob], "yyyy-mm-dd")
df[:encounter_date] = Date.(df[:encounter_date], "yyyy-mm-dd")
# reshape cohort year
start_dates = unstack(cohort_year, :_cohort_id, :year_id, :start_date)
end_dates = unstack(cohort_year, :_cohort_id, :year_id, :end_date)
rename!(start_dates, names(start_dates), [:_cohort_id, Symbol("start_0"), Symbol("start_1"), Symbol("start_2")])
rename!(end_dates, names(end_dates), [:_cohort_id, Symbol("end_0"), Symbol("end_1"), Symbol("end_2")])
int_dates = join(start_dates, end_dates, on=[:_cohort_id], kind=:inner)

# does child live in polygon?
for i in 1:size(df, 1)
    in_address_polygon(df[i, :address], df[i, :city])
    if length(polygon_id) < i
        in_pcp_polygon(df[i, :pcp])
    end
end
df[:polygon_id] = polygon_id

# according to polygon, which cohort and intervention year are they in?
df = join(df, cohort_polygon, on=[:polygon_id], kind=:left)
df = join(df, int_dates, on=[:_cohort_id], kind=:left)

# how old is child at start of intervention year?
for i in 1:size(df, 1)
    compute_age_at_start(df[i, :dob], df[i, :start_1])
end
df[:age_at_start] = age_at_start

# is encounter eligible or ineligible?
for i in 1:size(df, 1)
    eligibility(df[i, :age_at_start])
end
df[:eligible] = eligible

# are ineligible encounters eligible for community asthma programs?
# are eligible encounters in intervention year?
for i in 1:size(df, 1)
    if df[i, :eligible] == 0
        if df[i, :parent_dx] == 1
            push!(community_asthma, 1)
            push!(referral, 0)
            push!(outcome, 4)
            println("may be eligible for other community asthma programs")
        elseif df[i, :parent_dx] == 0
            push!(community_asthma, 0)
            push!(referral, 0)
            push!(outcome, 2)
            println("ineligible.")
        end
    elseif df[i, :eligible] == 1
        if intervention_year(df[i, :encounter_date], df[i, :start_1], df[i, :end_1]) == 1
            push!(community_asthma, missing)
            push!(referral, 1)
            push!(outcome, 1)
            println("initiate screening phone call")
        elseif intervention_year(df[i, :encounter_date], df[i, :start_1], df[i, :end_1]) == 0
            push!(community_asthma, missing)
            push!(referral, 0)
            push!(outcome, 3)
            println("holding pattern")
        elseif intervention_year(df[i, :encounter_date], df[i, :start_1], df[i, :end_1]) == 2
            push!(referral, 0)
            if df[i, :prev_participant] == 1
                push!(community_asthma, missing)
                push!(outcome, 2)
                println("ineligible for other community programs bc previous participant")
            elseif df[i, :prev_participant] == 0 
                if df[i, :parent_dx] == 0
                    push!(community_asthma, 1)
                    push!(outcome, 4)
                    println("may be eligible for other community asthma programs")
                elseif df[i, :parent_dx] == 1
                    push!(community_asthma, 0)
                    push!(outcome, 2)
                    println("ineligible for other community programs bc no parent dx")
                end
            end
        end
    end
end
df[:community_asthma] = community_asthma
df[:referral] = referral
df[:outcome] = outcome

# ------------------ reference tables
outcome_desc = DataFrame(outcome_id = collect(1:4),
                         outcome_desc = ["Eligible", "Ineligible", "Ineligible - holding pattern",
                                         "Ineligible - other community asthma programs"])

# ------------------ patients table
patients = unique(df[:, [:id, :dob]])

# ------------------ patient polygon table
patient_polygons = unique(df[:, [:id, :age_at_start, :polygon_id, :_cohort_id]])

# ------------------ encounters table
encounters = df[:, [:id, :encounter_date, :eligible, :community_asthma, :referral, :outcome]]

# ------------------ referral table and community asthma table

referrals = DataFrame(id=Int64[])
community_asthma = DataFrame(id=Int64[])

for i in 1:length(unique((df[:id])))
    subset = df[df[:id] .== i, :]
    if sum(subset[:outcome] .== 1) > 0
        push!(referrals, i)
        println("eligible")
    elseif sum(subset[:outcome] .== 4) > 0
        push!(community_asthma, i)
        println("ineligible")
    end
end

# ------------------ create a row ID primary key for each table
patient_polygons = add_row_id(patient_polygons)



# ------------------ write tables to MySQL database

# establish connection to mysql database and create database
conn = MySQL.connect("127.0.0.1", "root", "")
MySQL.execute!(conn, "DROP DATABASE if exists riairtest")
MySQL.execute!(conn, "CREATE DATABASE riairtest")
MySQL.execute!(conn, "USE riairtest")

df = DataFrame(id = id, 
              dob=dob,
              address = address,
              city = city,
              pcp = pcp,
              parent_dx = parent_dx,
              prev_participant = prev_participant,
              encounter_date = encounter_date
              )

# patients table
MySQL.execute!(conn, "DROP TABLE if exists patients")
MySQL.execute!(conn, """CREATE TABLE patients
(
    patient_id INT NOT NULL PRIMARY KEY,
    dob DATE
);""")
stmt = MySQL.Stmt(conn, "INSERT INTO patients (patient_id, dob) VALUES (?,?);")
df2mysql(patients, stmt)

# test data
MySQL.execute!(conn, "DROP TABLE if exists test_cases")
MySQL.execute!(conn, """CREATE TABLE test_cases
(
    patient_id INT NOT NULL,
    dob VARCHAR(255),
    home_address VARCHAR(255),
    city VARCHAR(255),
    pcp_name VARCHAR(255),
    parent_dx BOOL,
    prev_participant BOOL,
    encounter_date DATE,
    school_name VARCHAR(255),
    FOREIGN KEY fk_pid(patient_id)
    REFERENCES patients(patient_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
stmt = MySQL.Stmt(conn, "INSERT INTO test_data (patient_id, dob, home_address, city, pcp_name, parent_dx, prev_participant, encounter_date, school_name) VALUES (?,?,?,?,?,?,?,?,?);")
df2mysql(df, stmt)

# drop tables
MySQL.execute!(conn, "DROP TABLE if exists outcome_desc")
MySQL.execute!(conn, "DROP TABLE if exists year_desc")

MySQL.execute!(conn, "DROP TABLE if exists school_polygon")
MySQL.execute!(conn, "DROP TABLE if exists preschool_polygon")
MySQL.execute!(conn, "DROP TABLE if exists address_polygon")
MySQL.execute!(conn, "DROP TABLE if exists pcp_polygon")
MySQL.execute!(conn, "DROP TABLE if exists cohort_year")
MySQL.execute!(conn, "DROP TABLE if exists cohort_polygon")

MySQL.execute!(conn, "DROP TABLE if exists patient_polygons")
MySQL.execute!(conn, "DROP TABLE if exists encounters")
MySQL.execute!(conn, "DROP TABLE if exists referrals")
MySQL.execute!(conn, "DROP TABLE if exists community_asthma")

# create tables
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

MySQL.execute!(conn, """CREATE TABLE cohort_polygon
(
    cohort_id INT,
    polygon_id INT NOT NULL PRIMARY KEY
);""")
MySQL.execute!(conn, """CREATE TABLE school_polygon
(
    cohort_id INT,
    school_name VARCHAR(255) NOT NULL,
    polygon_id INT,
    city VARCHAR(255),
    PRIMARY KEY (school_name, city),
    FOREIGN KEY fk_polygon(polygon_id)
    REFERENCES cohort_polygon(polygon_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
MySQL.execute!(conn, """CREATE TABLE preschool_polygon
(
    cohort_id INT,
    school VARCHAR(255) NOT NULL,
    polygon_id INT,
    city VARCHAR(255) NOT NULL,
    PRIMARY KEY (school, city),
    FOREIGN KEY fk_polygon(polygon_id)
    REFERENCES cohort_polygon(polygon_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
MySQL.execute!(conn, """CREATE TABLE address_polygon
(
    fid INT NOT NULL PRIMARY KEY,
    object_id_1 INT,
    object_id_2 INT,
    update_date VARCHAR(255),
    comments TEXT,
    esite_id INT,
    m_code INT,
    site_type VARCHAR(255),
    zip INT,
    esn INT,
    measure INT,
    city VARCHAR(255),
    house_number INT,
    home_address VARCHAR(255),
    ali_address VARCHAR(255),
    primary_name VARCHAR(255),
    ali_name VARCHAR(255),
    alias1 VARCHAR(255),
    alias2 VARCHAR(255),
    alias3 VARCHAR(255),
    alias4 VARCHAR(255),
    alias5 VARCHAR(255),
    school VARCHAR(255),
    grades VARCHAR(255),
    polygon_id INT -- no foreign key because polygon_id can be 0 in this table
);""")
MySQL.execute!(conn, """CREATE TABLE pcp_polygon
(
    cohort_id INT,
    pcp_name VARCHAR(255) NOT NULL,
    pcp_address VARCHAR(255) NOT NULL,
    polygon_id INT,
    city VARCHAR(255),
    PRIMARY KEY (pcp_name, pcp_address),
    FOREIGN KEY fk_polygon(polygon_id)
    REFERENCES cohort_polygon(polygon_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
MySQL.execute!(conn, """CREATE TABLE cohort_year
(
    cohort_id INT NOT NULL,
    year_id INT NOT NULL,
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (cohort_id, year_id)
);""")

MySQL.execute!(conn, """CREATE TABLE patient_polygons
(
    row_id INT NOT NULL PRIMARY KEY,
    patient_id INT NOT NULL,
    age_at_start INT,
    polygon_id INT,
    cohort_id INT,
    FOREIGN KEY fk_pid(patient_id)
    REFERENCES patients(patient_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
MySQL.execute!(conn, """CREATE TABLE encounters
(
    patient_id INT NOT NULL,
    encounter_date DATE NOT NULL,
    eligible INT,
    community_asthma INT,
    referral INT,
    outcome INT,
    PRIMARY KEY (patient_id, encounter_date),
    FOREIGN KEY fk_pid(patient_id)
    REFERENCES patients(patient_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
MySQL.execute!(conn, """CREATE TABLE referrals
(
    patient_id INT NOT NULL,
    PRIMARY KEY (patient_id),
    FOREIGN KEY fk_pid(patient_id)
    REFERENCES patients(patient_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
MySQL.execute!(conn, """CREATE TABLE community_asthma
(
    patient_id INT NOT NULL,
    PRIMARY KEY (patient_id),
    FOREIGN KEY fk_pid(patient_id)
    REFERENCES patients(patient_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")

# insert dataframes into mysql tables
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
stmt = MySQL.Stmt(conn, "INSERT INTO address_polygon(fid, object_id_1, object_id_2, update_date, comments, esite_id, m_code, site_type, zip, esn, measure, city, house_number, home_address, ali_address, primary_name, ali_name, alias1, alias2, alias3, alias4, alias5, school, grades, polygon_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")
df2mysql(address_polygon, stmt) # TOFIX:
stmt = MySQL.Stmt(conn, "INSERT INTO pcp_polygon (cohort_id, pcp_name, pcp_address, polygon_id, city) VALUES (?,?,?,?,?);")
df2mysql(pcp_polygon, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO cohort_year (cohort_id, year_id, start_date, end_date) VALUES (?,?,?,?);")
df2mysql(cohort_year, stmt)

stmt = MySQL.Stmt(conn, "INSERT INTO patient_polygons (row_id, patient_id, age_at_start, polygon_id, cohort_id) VALUES (?,?,?,?,?);")
df2mysql(patient_polygons, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO encounters (patient_id, encounter_date, eligible, community_asthma, referral, outcome) VALUES (?,?,?,?,?,?);")
df2mysql(encounters, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO referrals (patient_id) VALUES (?);")
df2mysql(referrals, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO community_asthma (patient_id) VALUES (?);")
df2mysql(community_asthma, stmt)


# query tables
sql = "SELECT * FROM outcome_desc"
MySQL.Query(conn, sql)