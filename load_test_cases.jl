
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

# rename cohort_id in cohort_year table
rename!(cohort_year, f => t for (f, t) = zip([:_cohort_id], [:cohort_id]))
rename!(cohort_polygon, f => t for (f, t) = zip([:_cohort_id], [:cohort_id]))

# convert dates to Date type
cohort_year[:start_date] = Date.(cohort_year[:start_date], "yyyy-mm-dd")
cohort_year[:end_date] = Date.(cohort_year[:end_date], "yyyy-mm-dd")
df[:dob] = Date.(df[:dob], "yyyy-mm-dd")
df[:encounter_date] = Date.(df[:encounter_date], "yyyy-mm-dd")
# reshape cohort year
start_dates = unstack(cohort_year, :cohort_id, :year_id, :start_date)
end_dates = unstack(cohort_year, :cohort_id, :year_id, :end_date)
rename!(start_dates, f => t for (f, t) = zip([Symbol("0"),Symbol("1"),Symbol("2")], [Symbol("start_0"),Symbol("start_1"),Symbol("start_2")]))
rename!(end_dates, f => t for (f, t) = zip([Symbol("0"),Symbol("1"),Symbol("2")], [Symbol("end_0"),Symbol("end_1"),Symbol("end_2")]))
int_dates = join(start_dates, end_dates, on=[:cohort_id], kind=:inner)

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
df = join(df, int_dates, on=[:cohort_id], kind=:left)

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
            push!(community_asthma, 0)
            push!(referral, 1)
            push!(outcome, 1)
            println("initiate screening phone call")
        elseif intervention_year(df[i, :encounter_date], df[i, :start_1], df[i, :end_1]) == 0
            push!(community_asthma, 0)
            push!(referral, 0)
            push!(outcome, 3)
            println("holding pattern")
        elseif intervention_year(df[i, :encounter_date], df[i, :start_1], df[i, :end_1]) == 2
            push!(referral, 0)
            if df[i, :prev_participant] == 1
                push!(community_asthma, 0)
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

# ------------------ encounters table
encounters = df[:, [:id, :encounter_date, :age_at_start, :polygon_id, :cohort_id, :eligible, :community_asthma, :referral, :outcome]]


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




# ------------------ write tables to MySQL database

# establish connection to mysql database and create database
conn = MySQL.connect("127.0.0.1", "root", "")
MySQL.execute!(conn, "USE riair")

df = DataFrame(id = id, 
              dob=dob,
              address = address,
              city = city,
              pcp = pcp,
              parent_dx = parent_dx,
              prev_participant = prev_participant,
              encounter_date = encounter_date
              )

# drop tables
MySQL.execute!(conn, "SET FOREIGN_KEY_CHECKS=0;")
MySQL.execute!(conn, "DROP TABLE if exists patients")
MySQL.execute!(conn, "SET FOREIGN_KEY_CHECKS=1;")
MySQL.execute!(conn, "DROP TABLE if exists test_cases")
MySQL.execute!(conn, "DROP TABLE if exists encounters")
MySQL.execute!(conn, "DROP TABLE if exists referrals")
MySQL.execute!(conn, "DROP TABLE if exists community_asthma")


# create tables
MySQL.execute!(conn, """CREATE TABLE patients
(
    patient_id INT NOT NULL PRIMARY KEY,
    dob DATE
);""")
MySQL.execute!(conn, """CREATE TABLE test_cases
(
    patient_id INT NOT NULL,
    dob DATE,
    home_address VARCHAR(255),
    city VARCHAR(255),
    pcp_name VARCHAR(255),
    parent_dx BOOL,
    prev_participant BOOL,
    encounter_date DATE,
    insert_date DATE,
    FOREIGN KEY fk_pid(patient_id)
    REFERENCES patients(patient_id)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
);""")
MySQL.execute!(conn, """CREATE TABLE encounters
(
    patient_id INT NOT NULL,
    encounter_date DATE NOT NULL,
    age_at_start INT,
    polygon_id INT,
    cohort_id INT,
    eligible INT,
    community_asthma INT,
    referral INT,
    outcome INT,
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
stmt = MySQL.Stmt(conn, "INSERT INTO patients (patient_id, dob) VALUES (?,?);")
df2mysql(patients, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO test_cases (patient_id, dob, home_address, city, pcp_name, parent_dx, prev_participant, encounter_date, insert_date) VALUES (?,?,?,?,?,?,?,?,CURDATE());")
df2mysql(df, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO encounters (patient_id, encounter_date, age_at_start, polygon_id, cohort_id, eligible, community_asthma, referral, outcome) VALUES (?,?,?,?,?,?,?,?,?);")
df2mysql(encounters, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO referrals (patient_id) VALUES (?);")
df2mysql(referrals, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO community_asthma (patient_id) VALUES (?);")
df2mysql(community_asthma, stmt)

