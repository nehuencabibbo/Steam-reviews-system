# TODO: Poner el resto de querys 
# Como exportar de kraggle:
# df[["column1", "column2"]].to_csv('q5_result.csv', sep=',', encoding='utf-8', index=False, header=False)
mkdir -p data && mkdir -p data/results &&
docker logs client1 &> ./data/results/result.txt && 
python3 notebook.py && 
python3 compare_results.py data/results/result.txt data/results/q1_expected.csv Q1 &&
python3 compare_results.py data/results/result.txt data/results/q2_expected.csv Q2 &&
python3 compare_results.py data/results/result.txt data/results/q3_expected.csv Q3 && 
python3 compare_results.py data/results/result.txt data/results/q4_expected.csv Q4 &&
python3 compare_results.py data/results/result.txt data/results/q5_expected.csv Q5