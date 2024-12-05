# ATENTIS: con esto genero los archivos de c/ cliente (que igual no son de cada cliente pero weno xd)
# cat q1_counter0.txt | grep "2805c8bc-357b-49d8-b83c-a0ed729c4b63" > 2805c8bc-357b-49d8-b83c-a0ed729c4b63
file_path_1 = "2805c8bc-357b-49d8-b83c-a0ed729c4b63"
file_path_2 = "449b1360-a83a-4571-ae45-38dee009001d"
import ast

# Read the data into Python
with open(file_path_1, "r") as file1, open(file_path_2, "r") as file2:
    data_1 = file1.readlines()
    data_2 = file2.readlines()


batches1 = [
    data.replace("q1_counter0      | [DEBUG]   ", "").split("\n")
    for data in data_1
    if "GOT BATCH" in data
]

batches = [ast.literal_eval(batch[0].replace("GOT BATCH: ", "")) for batch in batches1]


x = []
for batch in batches:
    for b in batch:
        if b[0] == file_path_1 and b[2] == "WINDOWS":
            # print("b: ", b[1])
            x.append(b[1])

print("x:", len(x))


batches2 = [
    data.replace("q1_counter0      | [DEBUG]   ", "").split("\n")
    for data in data_2
    if "GOT BATCH" in data
]

batches2 = [ast.literal_eval(batch[0].replace("GOT BATCH: ", "")) for batch in batches2]


y = []

for batch in batches2:
    for b in batch:
        if b[0] == file_path_2 and b[2] == "WINDOWS":
            y.append(b[1])

print("y:", len(y))
