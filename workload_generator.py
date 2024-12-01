import csv
import random
import sys
from operator import index

# Define header and data values to be randomly selected from for the csv file
header_r = ['patient_id', 'name', 'city', 'state']
header_s = ['patient_id', 'doctor']
state_abbr = [
    'NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 'TX', 'FL',
    'TX', 'OH', 'NC', 'CA', 'IN', 'WA', 'CO', 'DC', 'MA', 'TX', 'MI', 'TN',
    'TN', 'OR', 'OK', 'NV', 'KY', 'MD', 'WI', 'NM', 'AZ', 'CA', 'CA', 'MO',
    'GA', 'FL', 'NE', 'NC', 'CO', 'CA', 'VA', 'CA', 'MN', 'OK', 'TX', 'FL',
    'LA', 'KS', 'OH'
]
patient_name = [
    'Liam', 'Charlotte', 'Oliver', 'Amelia', 'Noah', 'Sophia', 'Elijah', 'Isabella',
    'James', 'Mia', 'William', 'Ava', 'Lucas', 'Harper', 'Benjamin', 'Evelyn',
    'Henry', 'Abigail', 'Alexander', 'Ella', 'Michael', 'Elizabeth', 'Daniel', 'Sofia',
    'Jacob', 'Emily', 'Ethan', 'Madison', 'Matthew', 'Scarlett', 'Sebastian', 'Victoria',
    'Jackson', 'Luna', 'Levi', 'Grace', 'Mateo', 'Chloe', 'Jack', 'Zoey', 'Owen',
    'Aria', 'Gabriel', 'Hazel', 'Caleb', 'Ellie', 'Nathan', 'Nora', 'Samuel', 'Layla'
]

doctor_name = [
    'Johnson', 'Kim', 'Brown', 'Taylor', 'Anderson', 'Thomas',
    'Jackson', 'White', 'Harris', 'Martin', 'Thompson', 'Garcia',
    'Martinez', 'Robinson', 'Clark', 'Rodriguez', 'Lewis', 'Lee',
    'Walker', 'Hall', 'Allen', 'Young', 'King', 'Wright', 'Scott',
    'Adams', 'Baker', 'Gonzalez', 'Nelson', 'Carter'
]
cities = [
    'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
    'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
    'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis',
    'Seattle', 'Denver', 'Washington', 'Boston', 'El Paso', 'Detroit',
    'Nashville', 'Memphis', 'Portland', 'Oklahoma City', 'Las Vegas',
    'Louisville', 'Baltimore', 'Milwaukee', 'Albuquerque', 'Tucson', 'Fresno',
    'Sacramento', 'Kansas City', 'Atlanta', 'Miami', 'Omaha', 'Raleigh',
    'Colorado Springs', 'Long Beach', 'Virginia Beach', 'Oakland', 'Minneapolis',
    'Tulsa', 'Arlington', 'Tampa', 'New Orleans', 'Wichita', 'Cleveland'
]

filename_r = 'R.csv'
filename_s = 'S.csv'
random.seed(42)
# generate data for the R table
with open(filename_r, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Writing the header
    writer.writerow(header_r)
    for i in range(int(sys.argv[1])):
        # generate data for each row and pass to writerow function using a list
        row_patient_id = random.randint(1, 50)
        index_patient = row_patient_id - 1
        row_patient_name = patient_name[index_patient]
        row_city = cities[index_patient]
        row_state = state_abbr[index_patient]
        row_data = [row_patient_id, row_patient_name, row_city, row_state]
        writer.writerow(row_data)  # Writing the rows/data

# generate data for the S table
with open(filename_s, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Writing the header
    writer.writerow(header_s)
    for i in range(int(sys.argv[2])):
        # generate data for each row and pass to writerow function using a list
        row_patient_id = random.randint(1, 50)
        row_doctor_name = random.choice(doctor_name)
        row_data = [row_patient_id, row_doctor_name]
        writer.writerow(row_data)



