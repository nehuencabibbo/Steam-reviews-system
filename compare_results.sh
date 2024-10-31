# Como exportar de kraggle:
# df[["column1", "column2"]].to_csv('q5_result.csv', sep=',', encoding='utf-8', index=False, header=False)
client_number=$1
client="client${client_number}"

echo "Retrieving logs for ${client}"
docker logs ${client} &> ./data/results/result.txt
echo "Finished retrieving logs for ${client}"


echo "Creating data/results directory"
mkdir -p data && mkdir -p data/results 

if [ "$2" == "--re-run" ]; then
    echo "Re-running notebook"
    python3 notebook.py
fi
echo "Using existing results"

echo "Starting to compare results"
python3 compare_results.py data/results/result.txt data/results/q1_expected.csv Q1 &&
python3 compare_results.py data/results/result.txt data/results/q2_expected.csv Q2 &&
python3 compare_results.py data/results/result.txt data/results/q3_expected.csv Q3 && 
python3 compare_results.py data/results/result.txt data/results/q4_expected.csv Q4 &&
python3 compare_results.py data/results/result.txt data/results/q5_expected.csv Q5