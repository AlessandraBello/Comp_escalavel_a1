#include <iostream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <functional>
#include <future>

#include "df.h"
#include "threads.h"
#include "tratadores_S.h"

using namespace std;

// Tipo possível das variáveis
using ElementType = variant<int, float, bool, string>; 

// Mutex para sincronizar acesso ao DataFrame
mutex df_mutex;

template <typename T>
T accumulate(const vector<T>& vec, T init) {
    for (int i = 0; i < vec.size(); i++) {
        T val = vec[i];
        init += val;
    }
    return init;
}

// Função auxiliar para filtrar um bloco de registros
vector<int> filter_block_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int idx_min, int idx_max) {
    vector<int> idxes_list;
    
    for (int i = idx_min; i < idx_max; i++) {
        if (condition(df.getRecord(i))) {
            idxes_list.push_back(i);
        }
    }
    
    return idxes_list;
}

// Função auxiliar para criar um novo DataFrame com registros filtrados
DataFrame filter_records_by_idxes(DataFrame& df, const vector<int>& idxes) {
    return df.getRecords(idxes);
}

// Função principal para filtrar registros
DataFrame filter_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int num_threads) {
    int block_size = df.getNumRecords() / num_threads;
    
    vector<vector<int>> thread_results(num_threads);
    vector<thread> threads;
    
    df_mutex.lock();
    
    for (int i = 0; i < num_threads; i++) {
        int start = i * block_size;
        int end = (i == num_threads - 1) ? df.getNumRecords() : min(start + block_size, df.getNumRecords());
        
        threads.emplace_back([&, start, end, i]() {
            thread_results[i] = filter_block_records(df, condition, start, end);
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    df_mutex.unlock();
    
    // Combina os resultados
    vector<int> idx_validos;
    for (auto it = thread_results.begin(); it != thread_results.end(); ++it) {
        const auto& result = *it;
        idx_validos.insert(idx_validos.end(), result.begin(), result.end());
    }
    
    sort(idx_validos.begin(), idx_validos.end());
    
    return filter_records_by_idxes(df, idx_validos);
}

void func (int start, int end, DataFrame &df, vector<unordered_map<string, pair<float, int>>> &local_maps, int t, int group_idx, int target_idx) {
    unordered_map<string, pair<float, int>>& group_map = local_maps[t];
    vector<ElementType> groupCol = df.getColumn(group_idx);
    vector<ElementType> targetCol = df.getColumn(target_idx);
    for (int i = start; i < end; ++i) {
        string key = variantToString(groupCol[i]);
        float value = get<float>(targetCol[i]);

        pair<float, int>& entry = group_map[key];
        entry.first += value; // soma
        entry.second += 1;    // contagem
    }
    cout << "Thread " << t << " Completou a agrupagem" << endl;
}

DataFrame groupby_mean(DataFrame& df, const string& group_col, const string& target_col, int num_threads) {
    int group_idx = df.getColumnIndex(group_col);
    int target_idx = df.getColumnIndex(target_col);
    int n = df.getNumRecords();

    // Passo 1: paraleliza a agregação local de sum e count
    vector<unordered_map<string, pair<float, int>>> local_maps(num_threads);
    vector<thread> threads;

    int block_size = (n + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; ++t) {
        int start = t * block_size;
        int end = min(start + block_size, n);

        threads.push_back(thread(func, start, end, ref(df), ref(local_maps), t, group_idx, target_idx));
        cout << "Criou thread " << t << endl;
    }

    for (auto& th : threads) th.join();

    // Passo 2: unifica os mapas locais em um só
    unordered_map<string, pair<float, int>> global_map;

    for (int t = 0; t < num_threads; ++t) {
        for (const auto& [key, val] : local_maps[t]) {
            auto& entry = global_map[key];
            entry.first += val.first;
            entry.second += val.second;
        }
    }

    // Passo 3: constrói o novo DataFrame com as médias
    vector<string> colNames = {group_col, "mean_" + target_col};
    vector<string> colTypes = {"string", "float"};
    DataFrame result_df(colNames, colTypes);

    // Também paralelizado
    vector<pair<string, float>> results_buffer(global_map.size());
    size_t i = 0;
    for (const auto& [key, val] : global_map) {
        results_buffer[i++] = {key, val.first / val.second};
    }

    // Paraleliza a inserção no result_df (com lock por segurança)
    mutex mtx;
    size_t result_size = results_buffer.size();
    block_size = (result_size + num_threads - 1) / num_threads;
    threads.clear();

    for (int t = 0; t < num_threads; ++t) {
        size_t start = t * block_size;
        size_t end = min(start + block_size, result_size);
    
        threads.emplace_back([start, end, &results_buffer, &result_df, &mtx]() {
            for (size_t i = start; i < end; ++i) {
                const auto& [key, mean] = results_buffer[i];
                result_df.addRecord({key, to_string(mean)});  // <-- conversão aqui
            }
        });
    }    

    for (auto& th : threads) th.join();

    return result_df;
}

DataFrame join_by_key(const DataFrame& df1, const DataFrame& df2, const string& key_col, int num_threads) {
    size_t key_idx1 = df1.getColumnIndex(key_col);
    size_t key_idx2 = df2.getColumnIndex(key_col);

    const auto& key_col1 = df1.columns[key_idx1];
    const auto& key_col2 = df2.columns[key_idx2];

    // Pré-processamento: índice para busca em df2 baseado na chave
    unordered_map<int, vector<size_t>> df2_lookup;
    for (size_t i = 0; i < key_col2.size(); ++i) {
        if (holds_alternative<int>(key_col2[i])) {
            int key = get<int>(key_col2[i]);
            df2_lookup[key].push_back(i);
        }
    }

    // Construir metadados do DataFrame resultante
    vector<string> result_col_names;
    vector<string> result_col_types;

    for (const auto& name : df1.colNames) {
        result_col_names.push_back("A_" + name);
        result_col_types.push_back(df1.colTypes.at(name));
    }
    for (const auto& name : df2.colNames) {
        if (name != key_col) {
            result_col_names.push_back("B_" + name);
            result_col_types.push_back(df2.colTypes.at(name));
        }
    }

    DataFrame result(result_col_names, result_col_types);

    // Paralelismo
    size_t numRecords = df1.getNumRecords();
    size_t block_size = (numRecords + num_threads - 1) / num_threads;

    mutex mtx;
    vector<thread> threads;

    auto join_task = [&](size_t start, size_t end) {
        vector<vector<string>> local_records;

        for (size_t i = start; i < end && i < numRecords; ++i) {
            if (!holds_alternative<int>(key_col1[i])) continue;

            int key = get<int>(key_col1[i]);
            auto it = df2_lookup.find(key);
            if (it == df2_lookup.end()) continue;

            for (size_t match_idx : it->second) {
                vector<ElementType> joined_row;

                // Dados do df1
                for (size_t j = 0; j < df1.getNumCols(); ++j) {
                    joined_row.push_back(df1.columns[j][i]);
                }

                // Dados do df2 (sem a chave)
                for (size_t j = 0; j < df2.getNumCols(); ++j) {
                    if (j == key_idx2) continue;
                    joined_row.push_back(df2.columns[j][match_idx]);
                }

                // Conversão para string
                vector<string> record_str;
                for (const auto& el : joined_row) {
                    if (holds_alternative<int>(el)) {
                        record_str.push_back(to_string(get<int>(el)));
                    } else if (holds_alternative<float>(el)) {
                        record_str.push_back(to_string(get<float>(el)));
                    } else {
                        record_str.push_back(get<string>(el));
                    }
                }

                local_records.push_back(record_str);
            }
        }

        // Inserção protegida no DataFrame resultante
        mtx.lock();
        for (const auto& record : local_records) {
            result.addRecord(record);
        }
        mtx.unlock();
    };

    // Lançar threads
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start = i * block_size;
        size_t end = min(start + block_size, numRecords);
        threads.emplace_back(join_task, start, end);
    }

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }

    return result;
}

namespace multiprocessing{
    DataFrame filter_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int num_threads, ThreadPool& pool) {
        // Define o tamanho do bloco
        int block_size = (df.getNumRecords() + num_threads - 1) / num_threads;

        // Define vetor de promessas e de futuros
        vector<promise<vector<int>>> promises(num_threads);
        vector<future<vector<int>>> futures;

        // Conectamos promessas e futuros (future resolvido qnd chamarmos p.set_value())
        for (auto& p : promises) {
            futures.push_back(p.get_future()); // Armazenamos todos os leitores para uso posterior
        }

        // Para cada thread
        for (int i = 0; i < num_threads; ++i) {
            // Define os limites do bloco
            int start = i * block_size;
            int end = min(start + block_size, df.getNumRecords());

            // Cada thread processa uma lambda, que processa um bloco
            pool.enqueue(
                [&, start, end, i]() mutable {
                    // Recebemos o vetor filtrado e comunicamos às promessas
                    vector<int> result = filter_block_records(df, condition, start, end);
                    promises[i].set_value(move(result));
                }
            );
        }

        // Espera todos os resultados e junta os índices válidos
        vector<int> idx_validos;
        for (auto& f : futures) {
            vector<int> res = f.get();
            idx_validos.insert(idx_validos.end(), res.begin(), res.end());
        }

        sort(idx_validos.begin(), idx_validos.end());
        return filter_records_by_idxes(df, idx_validos);
    }

    DataFrame groupby_mean(DataFrame& df, const string& group_col, const string& target_col, ThreadPool& pool) {
        // Toma os índices das colunas relevantes
        int group_idx = df.getColumnIndex(group_col);
        int target_idx = df.getColumnIndex(target_col);
    
        // Acessa as colunas de agrupamento e a coluna cujos valores vamos operar
        const auto& group_vec = df.columns[group_idx];
        const auto& target_vec = df.columns[target_idx];
    
        int total_records = df.getNumRecords();
        int num_threads = pool.size();
        int block_size = (total_records + num_threads - 1) / num_threads;
    
        // Cria promessas e futuros
        vector<promise<unordered_map<string, pair<float, int>>>> promises(num_threads);
        vector<future<unordered_map<string, pair<float, int>>>> futures;
    
        // Define o relacionamento entre promessas e futuros
        for (auto& p : promises) {
            futures.push_back(p.get_future());
        }
    
        // Para cada thread
        for (int t = 0; t < num_threads; ++t) {
            int start = t * block_size;
            int end = min(start + block_size, total_records);
            
            pool.enqueue([&, start, end, t]() mutable {
                unordered_map<string, pair<float, int>> local_map;
    
                for (int i = start; i < end; ++i) {
                    string key = variantToString(group_vec[i]); // converte o valor da coluna de agrupamento para string
                    float value = get<float>(target_vec[i]);    // extrai o valor numérico da coluna de interesse
                    auto& acc = local_map[key]; // pega ref do acumulador da chave
                    acc.first += value;
                    acc.second += 1;
                }
    
                // Entrega o resultado parcial da thread via promise
                promises[t].set_value(move(local_map));
            });
        }
    
        // Fusão dos resultados: chave -> (soma total, contagem total)
        unordered_map<string, pair<float, int>> global_map;

        // Para cada futuro
        for (auto& f : futures) {
            auto local = f.get(); // obtém o resultado parcial da thread (bloqueia até o valor estar disponível)
            // Adiona o resultado local no mapa global
            for (const auto& [key, val] : local) {
                global_map[key].first += val.first;
                global_map[key].second += val.second;
            }
        }
    
        // Montagem do DataFrame de saída
        vector<string> colNames = {group_col, "mean_" + target_col};
        vector<string> colTypes = {"string", "float"};
        DataFrame result_df(colNames, colTypes);
    
        for (const auto& [key, pair] : global_map) {
            float mean = pair.first / pair.second;
            result_df.addRecord({key, to_string(mean)});    // adiciona ao DataFrame como registro
        }
    
        return result_df;
    }
    
    DataFrame join_by_key(const DataFrame& df1, const DataFrame& df2, const string& key_col, ThreadPool& pool) {
        // Pega os índices das colunas relevantes
        size_t key_idx1 = df1.getColumnIndex(key_col);
        size_t key_idx2 = df2.getColumnIndex(key_col);
    
        // Acessa as colunas de interesse
        const auto& key_col1 = df1.columns[key_idx1];
        const auto& key_col2 = df2.columns[key_idx2];
    
        // Pré-processamento: índice para busca em df2 baseado na chave
        unordered_map<int, vector<size_t>> df2_lookup;
        // Mapa que associa cada valor da chave em df2 a uma lista de posições onde ele aparece

        for (size_t i = 0; i < key_col2.size(); ++i) {
            // Verifica se o valor da chave naquela linha é do tipo int
            if (holds_alternative<int>(key_col2[i])) {
                int key = get<int>(key_col2[i]);         // Extrai o valor inteiro da chave
                df2_lookup[key].push_back(i);            // Adiciona o índice i à lista de posições dessa chave
            }
        }
    
        // Construir metadados do DataFrame resultante
        vector<string> result_col_names;
        vector<string> result_col_types;
    
        // Mudamos os nomes das colunas para podermos entender sua origem quando virmos o dataframe depois do join
        for (const auto& name : df1.colNames) {
            result_col_names.push_back("A_" + name);
            result_col_types.push_back(df1.colTypes.at(name));
        }
        for (const auto& name : df2.colNames) {
            if (name != key_col) {
                result_col_names.push_back("B_" + name);
                result_col_types.push_back(df2.colTypes.at(name));
            }
        }
    
        DataFrame result(result_col_names, result_col_types);
    
        // Paralelismo usando ThreadPool
        size_t numRecords = df1.getNumRecords();
        int num_threads = pool.size();
        size_t block_size = (numRecords + num_threads - 1) / num_threads;
    
        vector<future<vector<vector<string>>>> futures;
    
        for (int i = 0; i < num_threads; ++i) {
            size_t start = i * block_size;
            size_t end = min(start + block_size, numRecords);
    
            promise<vector<vector<string>>> p;
            futures.push_back(p.get_future());
    
            pool.enqueue([&, start, end, p = move(p)]() mutable {
                vector<vector<string>> local_records;
    
                for (size_t i = start; i < end && i < numRecords; ++i) {
                    if (!holds_alternative<int>(key_col1[i])) continue;
    
                    int key = get<int>(key_col1[i]);
                    auto it = df2_lookup.find(key);
                    if (it == df2_lookup.end()) continue;
    
                    for (size_t match_idx : it->second) {
                        vector<ElementType> joined_row;
    
                        // Dados do df1
                        for (size_t j = 0; j < df1.getNumCols(); ++j) {
                            joined_row.push_back(df1.columns[j][i]);
                        }
    
                        // Dados do df2 (sem a chave)
                        for (size_t j = 0; j < df2.getNumCols(); ++j) {
                            if (j == key_idx2) continue;
                            joined_row.push_back(df2.columns[j][match_idx]);
                        }
    
                        // Conversão para string
                        vector<string> record_str;
                        for (const auto& el : joined_row) {
                            if (holds_alternative<int>(el)) {
                                record_str.push_back(to_string(get<int>(el)));
                            } else if (holds_alternative<float>(el)) {
                                record_str.push_back(to_string(get<float>(el)));
                            } else {
                                record_str.push_back(get<string>(el));
                            }
                        }
    
                        local_records.push_back(record_str);
                    }
                }
    
                p.set_value(move(local_records));
            });
        }
    
        // Fusão dos resultados
        for (auto& f : futures) {
            auto local = f.get();
            for (const auto& record : local) {
                result.addRecord(record);
            }
        }
    
        return result;
    }
}

#include "csv_extractor.h"
#include <chrono>
using namespace std::chrono;

int main() {
    string file1 = "transaction_4_2.csv";
    string file2 = "transaction_4_2.csv";
    string file3 = "transactions.csv";

    vector<int> thread_counts = {1, 2, 4, 8, 16};

    for (int num_threads : thread_counts) {
        cout << "\n======================" << endl;
        cout << "   Threads: " << num_threads << endl;
        cout << "======================\n" << endl;

        ThreadPool tp(num_threads);

        auto start = high_resolution_clock::now();
        DataFrame* df1 = readCSV(file1, num_threads);
        DataFrame* df2 = readCSV(file2, num_threads);
        DataFrame* df = readCSV(file3, num_threads);
        auto end = high_resolution_clock::now();

        cout << "[readCSV] Tempo total: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        start = high_resolution_clock::now();
        DataFrame joined = multiprocessing::join_by_key(*df1, *df2, "account_id", tp);
        end = high_resolution_clock::now();
        cout << "[join_by_key] Tempo: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        auto condition = [df](const vector<ElementType>& row) -> bool {
            float amount = get<float>(row[df->getColumnIndex("amount")]);
            string type = get<string>(row[df->getColumnIndex("type")]);
            return amount > 1000 && type == "depósito";
        };

        start = high_resolution_clock::now();
        DataFrame filtered = multiprocessing::filter_records(*df, condition, num_threads, tp);
        end = high_resolution_clock::now();
        cout << "[filter_records] Tempo: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        start = high_resolution_clock::now();
        DataFrame grouped = multiprocessing::groupby_mean(*df, "location", "amount", tp);
        end = high_resolution_clock::now();
        cout << "[groupby_mean] Tempo: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        if (num_threads == thread_counts.back()) {
            joined.DFtoCSV("joined" + num_threads);
            filtered.DFtoCSV("filtered" + num_threads);
            grouped.DFtoCSV("grouped" + num_threads);
        }

        delete df1;
        delete df2;
        delete df;
    }

    return 0;
}
