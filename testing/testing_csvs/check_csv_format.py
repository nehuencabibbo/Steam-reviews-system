import csv

def read_csv_and_create_dict(file_path): 
    with open(file_path, mode='r', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        
        header = next(csvreader)
        line = next(csvreader)
        for _ in range(10):
            line = next(csvreader)
        print(line)
        data_dict = {header[i]: line[i] for i in range(len(header))}
        
        for key, value in data_dict.items():
            pass
            #print(f"{key}: {value}")

# Example usage
file_path = "/home/nehuen/Desktop/distribuidos/steam_reviews_system/data/games.csv" # Replace with your CSV file path
read_csv_and_create_dict(file_path)