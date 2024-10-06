import csv

def read_csv_and_print_dict(file_path):
    with open(file_path, mode='r', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        
        # Read the header
        header = next(csvreader)
        
        # Read the first line
        first_line = next(csvreader)
        
        # Create dictionary
        header_value_dict = dict(zip(header, first_line))
        
        # Print each key-value pair on separate lines
        for key, value in header_value_dict.items():
            print(f"{key}: {value}")

if __name__ == "__main__":
    # Replace 'your_file.csv' with the path to your CSV file
    read_csv_and_print_dict('/home/nehuen/Desktop/distribuidos/steam_reviews_system/data/games.csv')