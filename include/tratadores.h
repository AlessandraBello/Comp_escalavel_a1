#ifndef TRATADORES
#define TRATADORES

//#pragma once
#include <vector>
#include <functional>
#include <string>
#include "df.h"
#include "threads.h"
using namespace std;

vector<int> filter_block_records(const class DataFrame& df, function<bool(const vector<ElementType>&)> condition, int idx_min, int idx_max);

class DataFrame filter_records_by_idxes(const class DataFrame& df, const vector<int>& idxes);

vector<int> filter_block_records(DataFrame& df, int id, int numThreads, function<bool(const vector<ElementType>&)> condition, int idxMin, int idxMax);
DataFrame filter_records_by_idxes(DataFrame& df, int id, int numThreads, const vector<int>& idxes);
DataFrame filter_records(DataFrame& df, int id, int numThreads, function<bool(const vector<ElementType>&)> condition, ThreadPool& pool);
DataFrame groupby_mean(DataFrame& df, int id, int numThreads, const string& groupCol, const string& targetCol, ThreadPool& pool);
DataFrame join_by_key(const DataFrame& df1, const DataFrame& df2, int id, int numThreads, const string& keyCol, ThreadPool& pool);
DataFrame count_values(const DataFrame& df, int id, int numThreads, const string& colName, int numDays, ThreadPool& pool);
DataFrame get_hour_by_time(const DataFrame& df, int id, int numThreads, const string& colName, ThreadPool& pool);
DataFrame num_transac_by_hour(const DataFrame& df, int id, int numThreads, const string& hourCol, int numDays, ThreadPool& pool);
DataFrame classify_accounts_parallel(DataFrame& df, int id, int numThreads, const string& idCol, const string& classFirst, const string& classSec, ThreadPool& tp);
DataFrame sort_by_column_parallel(const DataFrame& df, int id, int numThreads, const string& keyCol, ThreadPool& pool, bool ascending);
unordered_map<string, ElementType> getQuantiles(const DataFrame& df, int id, int numThreads, const string& colName, const vector<double>& quantiles, ThreadPool& pool);
double calculateMeanParallel(const DataFrame& df, int id, int numThreads, const string& targetCol, ThreadPool& pool);
DataFrame summaryStats(const DataFrame& df, int id, int numThreads, const string& colName, ThreadPool& pool);
DataFrame top_10_cidades_transacoes(const DataFrame& df, int id, int numThreads, const string& colName, ThreadPool& pool);
DataFrame abnormal_transactions(const DataFrame& dfTransac, const DataFrame& dfAccount, int id, int numThreads, const string& transactionIDCol, const string& amountCol, const string& locationColTransac, const string& accountColTransac, const string& accountColAccount, const string& locationColAccount, ThreadPool& pool);

#endif