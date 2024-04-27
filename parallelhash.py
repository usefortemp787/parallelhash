import threading
from prettytable import PrettyTable

students_data = [
    {"PRN NO.": 101, "Name": "Aarav Sharma", "Class": "FY", "Branch": "Computer Engineering"},
    {"PRN NO.": 102, "Name": "Aditi Singh", "Class": "SY", "Branch": "Information Technology"},
    {"PRN NO.": 103, "Name": "Vivek Patel", "Class": "TY", "Branch": "Electronics Engineering"},
    {"PRN NO.": 104, "Name": "Sneha Krishnan", "Class": "FY", "Branch": "Mechanical Engineering"},
    {"PRN NO.": 105, "Name": "Rohan Gupta", "Class": "SY", "Branch": "Civil Engineering"},
    {"PRN NO.": 106, "Name": "Pooja Desai", "Class": "TY", "Branch": "Electrical Engineering"},
    {"PRN NO.": 107, "Name": "Nikhil Joshi", "Class": "FY", "Branch": "Chemical Engineering"},
]

performance_data = [
    {"PRN NO.": 101, "Attendance": "90%", "Grade": "A"},
    {"PRN NO.": 102, "Attendance": "85%", "Grade": "B"},
    {"PRN NO.": 103, "Attendance": "92%", "Grade": "A"},
    {"PRN NO.": 104, "Attendance": "88%", "Grade": "B"},
    {"PRN NO.": 105, "Attendance": "95%", "Grade": "A"},
    {"PRN NO.": 107, "Attendance": "75%", "Grade": "D"},
]

# Function to split (partition) the data using hash function h1
def split_data(data, num_partitions):
    partitions = [[] for _ in range(num_partitions)]
    for record in data:
        prn_no = record["PRN NO."]
        partition_id = h1(prn_no, num_partitions)
        partitions[partition_id].append(record)
    return partitions

# Function to perform the join using hash function h2
def join_data(students, partition, partition_id, result):
    in_memory_hash = {}
    for record in partition:
        prn_no = record["PRN NO."]
        in_memory_hash[prn_no] = record
    for student in students:
        prn_no = student["PRN NO."]
        processor_id = h2(prn_no, 4)  # Using 4 as the number of processors
        if prn_no in in_memory_hash and processor_id == partition_id:
            record = in_memory_hash[prn_no]
            result.append({
                "PRN NO.": prn_no,
                "Name": student["Name"],
                "Class": student["Class"],
                "Branch": student["Branch"],
                "Attendance": record["Attendance"],
                "Grade": record["Grade"]
            })

# Hash function h1 to partition the data
def h1(prn_no, num_partitions):
    return hash(prn_no) % num_partitions

# Hash function h2 to determine where tuples should be joined
def h2(prn_no, num_processors):
    return hash(prn_no) % num_processors

# Number of partitions
num_partitions = 4

# Split the performance data using hash function h1
partitions = split_data(performance_data, num_partitions)

# Perform join on partitioned data using threads
threads = []
results = []

for i, partition in enumerate(partitions):
    thread = threading.Thread(target=join_data, args=(students_data, partition, i, results))
    threads.append(thread)
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

# Display the result in a table format
result_table = PrettyTable(["PRN NO.", "Name", "Class", "Branch", "Attendance", "Grade"])
for data in results:
    result_table.add_row([
        data["PRN NO."],
        data["Name"],
        data["Class"],
        data["Branch"],
        data["Attendance"],
        data["Grade"]
    ])

print("Improved Parallel Hash Join Result:")
print(result_table)
