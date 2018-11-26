#!/usr/bin/env julia

using DataFrames
using MySQL
using RCall
using CSV


function input(prompt::String="")::String
    println(prompt)
    return(chomp(readline()))
end

function input_csv(prompt::String="")
    println(prompt)
    input_file = chomp(readline())
    return(readtable("./data/$(input_file)"))
end

function process_input(input)
    output = strip(lowercase(input))
    return output
end

function string2date(string)
    date = Date.(string, "yyyy-mm-dd")
    return date
end

function string2int(string)
    int = parse(Int64, string)
    return int
end

function string2bin(string)
    if string == "yes"
        bin = 1
    elseif string == "no"
        bin = 0
    end
    return bin
end

function df2mysql(df, stmt)
    for i in 1:size(df, 1)
        d = vec(convert(Array, df[i,:]))
        MySQL.execute!(stmt, d)
    end
end

function in_address_polygon(address, city)
    poly_id = unique(address_polygon[(address_polygon[:PrimaryAdd] .== address) .& (address_polygon[:ZN] .== city), :Poly_ID])
    if length(poly_id) > 0
        poly_id = poly_id[1]
        push!(polygon_id, poly_id)
        println("address in polygon $(poly_id)")
    elseif length(poly_id) == 0
        println("address not in a polygon")
    end
end

function in_pcp_polygon(pcp_id)
    poly_id = unique(pcp_polygon[pcp_polygon[:pcp_id] .== pcp_id, :polygon_id])
    if pcp_id in pcp_polygon[:pcp_id]
        poly_id = poly_id[1]
        push!(polygon_id, poly_id)
        println("pcp in polygon $(poly_id)")
    elseif (pcp_id in pcp_polygon[:pcp_id]) == false
        push!(polygon_id, missing)
        println("pcp not in a polygon")
    end
end

function compute_age_at_start(dob, das)
    if ismissing(das) == false
        aas = floor(convert(Int64, das - dob) / 365)
        push!(age_at_start, aas)
        println("age at start of intervention period = $(aas)")
    else
        push!(age_at_start, missing)
        println("cannot compute age at start of intervention period - not in an eligible polygon")
    end
end

function eligibility(age)
    if ismissing(age) == false
        if age >= 2 && age <= 12
            push!(eligible, 1)
            println("eligible - check intervention period")
        else
        push!(eligible, 0)
        println("ineligible for RI-Air because of age")
        end
    else
        push!(eligible, 0)
        println("ineligible for RI-Air because patient is not in an eligible polygon")
    end
end

function previous_participant(id, encounters)
    if size(encounters[(encounters[:patient_id] .== id) .& (encounters[:outcome] .== 1), :], 1) > 0
        push!(prev, 1)
        println("patient was previously eligible for RI-Air")
    else
        push!(prev, 0)
        println("patient was not previously eligible for RI-Air")
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


# ------------------ user input
how = input("---LOAD CSV FILE OR MANUAL DATA ENTRY? (csv=0, manual=1)---")

if how == "1"
    # ------------------ user input - Manual Entry
    new_cases = DataFrame(id = Int64[], 
                        dob = Date[],
                        home_address = String[],
                        city = String[],
                        pcp_name = Int64[],
                        parent_dx = Int64[],
                        encounter_date = Date[])
    cont = "yes"

    while cont == "yes"

        row = []

        id = input("---ENTER PATIENT ID---")
        push!(row, string2int(process_input(id)))

        dob = input("---ENTER DATE OF BIRTH (YYYY-MM-DD)---")
        push!(row, string2date(process_input(dob)))

        address = input("---ENTER HOME ADDRESS (ex: 56 Courtland St)---")
        push!(row, process_input(address))

        city = input("---ENTER CITY---")
        push!(row, process_input(city))

        pcp = input("---ENTER PCP (ID number)---")
        push!(row, string2int(process_input(pcp)))

        pdx = input("---IS THERE A PARENTAL ASTHMA DIAGNOSIS (yes/no)?---")
        push!(row, string2bin(process_input(pdx)))

        encounter = input("---ENTER ENCOUNTER DATE (YYYY-MM-DD)---")
        push!(row, string2date(process_input(encounter)))

        push!(new_cases, row)

        cont = input("---ADD MORE PATIENTS? (yes/no)---")

    end

    println("---DETERMINING ELIGIBILITY...")

elseif how == "0"
    # ------------------ user input - CSV
    new_cases = input_csv("---ENTER FILENAME (ex: new_cases.csv)---")
    new_cases[:dob] = Date.(new_cases[:dob])
    new_cases[:encounter_date] = Date.(new_cases[:encounter_date])

    println("---DETERMINING ELIGIBILITY...")

end


# ------------------ pull data tables
conn = MySQL.connect("127.0.0.1", "root", "")
MySQL.execute!(conn, "USE riair")

# previous test cases
sql = "SELECT * FROM test_cases"
test_cases = MySQL.query(conn, sql, DataFrame)

sql = "SELECT * FROM encounters"
encounters = MySQL.query(conn, sql, DataFrame)

# reference tables
sql = "SELECT * FROM pcp_polygon"
pcp_polygon = MySQL.query(conn, sql, DataFrame)

sql = "SELECT * FROM year_desc"
year_desc = MySQL.query(conn, sql, DataFrame)

sql = "SELECT * FROM cohort_year"
cohort_year = MySQL.query(conn, sql, DataFrame)

sql = "SELECT * FROM cohort_polygon"
cohort_polygon = MySQL.query(conn, sql, DataFrame)

R"address_polygon = read.csv('~/repos/riair/data/E911_Polygon_Table.csv', na.strings = c('', ' '))"
R"address_polygon[] <- lapply(address_polygon, function(x) if(is.factor(x)) as.character(x) else x)"
@rget address_polygon
address_polygon[:PrimaryAdd] = lowercase.(strip.(address_polygon[:PrimaryAdd]))
address_polygon[:ZN]= lowercase.(strip.(address_polygon[:ZN]))


# ------------------ debugging data
# id = [13, 14]
# dob = ["2016-07-01","2017-07-01"]
# address = ["56 Courtland St", "23 Ring St"]
# city = ["Providence", "Providence",]
# pcp = [2, 3]
# parent_dx = [0, 1]
# encounter_date = ["2019-01-01","2017-03-01"]
# new_cases = DataFrame(id = id, 
#               dob=dob,
#               home_address = address,
#               city = city,
#               pcp_name = pcp,
#               parent_dx = parent_dx,
#               encounter_date = encounter_date
#               )
# CSV.write("./data/new_cases.csv", new_cases)


# ------------------ logic
df = new_cases

polygon_id = []
age_at_start = []
eligible = []
community_asthma = []
referral = []
outcome = []
prev = []

# reshape cohort year
start_dates = unstack(cohort_year, :cohort_id, :year_id, :start_date)
end_dates = unstack(cohort_year, :cohort_id, :year_id, :end_date)
rename!(start_dates, f => t for (f, t) = zip([Symbol("0"),Symbol("1"),Symbol("2")], [Symbol("start_0"),Symbol("start_1"),Symbol("start_2")]))
rename!(end_dates, f => t for (f, t) = zip([Symbol("0"),Symbol("1"),Symbol("2")], [Symbol("end_0"),Symbol("end_1"),Symbol("end_2")]))
int_dates = join(start_dates, end_dates, on=[:cohort_id], kind=:inner)

# does child live in polygon?
for i in 1:size(df, 1)
    in_address_polygon(df[i, :home_address], df[i, :city])
    if length(polygon_id) < i
        in_pcp_polygon(df[i, :pcp_name])
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

# was child a previous RI Air participant
if size(encounters) == (0, 0)
    df[:prev_participant] = 0
elseif (size(encounters) == (0, 0)) == false
    for i in 1:size(df, 1)
        previous_participant(df[i, :id], encounters)
    end
    df[:prev_participant] = prev
end

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

# ------------------ outcome tables
# patients table
patients = unique(df[:, [:id, :dob]])

# encounters table
encounters = df[:, [:id, :encounter_date, :age_at_start, :polygon_id, :cohort_id, :eligible, :community_asthma, :referral, :outcome]]


# referral table and community asthma table
referrals = DataFrame(id=Int64[])
community_asthma = DataFrame(id=Int64[])

for i in unique(df[:id])
    subset = df[df[:id] .== i, :]
    if sum(subset[:outcome] .== 1) > 0
        push!(referrals, i)
        println("eligible for RI-Air")
    elseif sum(subset[:outcome] .== 4) > 0
        push!(community_asthma, i)
        println("may be eligible for other community asthma programs")
    else
        println("not eligible for RI-Air or other other community asthma programs")
    end
end

# write new cases to database with added previous participant field
new_cases = df[:, [:id, :dob, :home_address, :city, :pcp_name, :parent_dx, :prev_participant, :encounter_date]]



println("======================================================")
# output the referrals dataframe joined with the encounters dataframe to standard out
referrals_joined = join(referrals, encounters, on=[:id], kind=:inner)
println("")
println("ELIGIBLE FOR RI-AIR - INITIATE SCREENING PHONE CALL:")
println("")
showall(referrals_joined)
println("")
# output the community asthma dataframe joined with the encounters dataframe to standard out
community_joined = join(community_asthma, encounters, on=[:id], kind=:inner)
println("")
println("MAY BE ELIGIBLE FOR OTHER COMMUNITY ASTHMA PROGRAMS:")
println("")
showall(community_joined)
println("")


# ------------------ write todays patients into database
stmt = MySQL.Stmt(conn, "INSERT IGNORE INTO patients (patient_id, dob) VALUES (?,?);")
df2mysql(patients, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO test_cases (patient_id, dob, home_address, city, pcp_name, parent_dx, prev_participant, encounter_date, insert_date) VALUES (?,?,?,?,?,?,?,?,CURDATE());")
df2mysql(new_cases, stmt)
stmt = MySQL.Stmt(conn, "INSERT INTO encounters (patient_id, encounter_date, age_at_start, polygon_id, cohort_id, eligible, community_asthma, referral, outcome) VALUES (?,?,?,?,?,?,?,?,?);")
df2mysql(encounters, stmt)
stmt = MySQL.Stmt(conn, "REPLACE INTO referrals (patient_id) VALUES (?);")
df2mysql(referrals, stmt)
stmt = MySQL.Stmt(conn, "REPLACE INTO community_asthma (patient_id) VALUES (?);")
df2mysql(community_asthma, stmt)


# ------------------ write todays patients and outcomes to a csv
sql = "SELECT * FROM (SELECT * FROM encounters WHERE patient_id IN (SELECT DISTINCT patient_id FROM test_cases WHERE insert_date = CURDATE())) AS tmp
INNER JOIN outcome_desc ON tmp.outcome = outcome_desc.outcome_id;"
todays_patients = MySQL.query(conn, sql, DataFrame)
todays_date = Dates.today()
CSV.write("./report_$(todays_date).csv", todays_patients)


# TO DO:
# change print statements
