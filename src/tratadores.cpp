#include <iostream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <functional>
#include <future>
#include <cmath>
#include <numeric>

#include "../include/df.h"
#include "../include/threads.h"
#include "../include/tratadores.h"

using namespace std;

// Tipo possível das variáveis
using ElementType = variant<int, float, bool, string>; 

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
    vector<string> colTypes = {"int", "float"};
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

DataFrame count_values(const DataFrame& df, const string& colName, ThreadPool& pool)
{
    int colIdx = df.getColumnIndex(colName);
    vector<ElementType> column = df.getColumn(colIdx);
    size_t dataSize = column.size();

    int numThreads = pool.size();
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;

    vector<future<unordered_map<string, int>>> futures;

    // Cálculo de contagens para cada thread
    for (int t=0; t<numThreads; t++)
    {
        size_t start = t * blockSize;
        size_t end = min(start + blockSize, dataSize);
        if (start >= end) break;

        // Criando uma promise para um future
        auto promise = make_shared<std::promise<unordered_map<string, int>>>();
        futures.push_back(promise->get_future());

        // Enfileira tarefa no threadpool
        pool.enqueue([start, end, &column, promise]() {
            // Contagem
            unordered_map<string, int> localCounts;
            for (size_t i = start; i < end; i++) {
                const ElementType& val = column[i];
                string valStr = variantToString(val);
                localCounts[valStr]++;
            }
            // Desbloqueia o future e envia o valor a ele
            promise->set_value(move(localCounts));
        });
    }

     // Junta os resultados
     unordered_map<string, int> counts;
     for (auto& f : futures) {
        // f.get() é bloqueado até que a thred mande o valor da promise ao future
        unordered_map<string, int> localMap = f.get();
        for (const auto& pair : localMap) {
            counts[pair.first] += pair.second;
         }
     }

    // Novas colunas
    vector<ElementType> values;
    vector<ElementType> frequencies;

    // Adicionando valores às colunas
    for (const auto& pair: counts)
    {
        values.push_back(pair.first);
        frequencies.push_back(pair.second);
    }

    // Pegando tipo da coluna original
    int idxColumn = df.getColumnIndex(colName);
    string typeColumn = df.getColumnType(idxColumn);

    // Novo DataFrame
    vector<string> colNames = {"value", "count"};
    vector<string> colTypes = {typeColumn, "int"};

    DataFrame result(colNames, colTypes);
    result.addColumn(values, "value", typeColumn);
    result.addColumn(frequencies, "count", "int");

    return result;
}

DataFrame get_hour_by_time(const DataFrame& df, const string& colName, ThreadPool& pool)
{
    int idxColumn = df.getColumnIndex(colName);
    vector<ElementType> timeColumn = df.getColumn(idxColumn);

    size_t dataSize = timeColumn.size();
    int numThreads = pool.size();
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;
    vector<future<vector<string>>> futures;

    for (int t = 0; t < numThreads; ++t)
    {
        size_t start = t * blockSize;
        size_t end = min(start + blockSize, dataSize);
        if (start >= end) break;

        auto promise = make_shared<std::promise<vector<string>>>();
        futures.push_back(promise->get_future());

        // Enfileira a tarefa
        pool.enqueue([start, end, &timeColumn, promise]() {
            vector<string> partialResult;
            //partialResult.reserve(end - start); // pode ser útil para o desempenho

            for (size_t i = start; i < end; i++)
            {
                const auto& elem = timeColumn[i];
                if (holds_alternative<string>(elem))
                {
                    string timeStr = get<string>(elem);
                    partialResult.push_back(timeStr.substr(0, 2));
                }
                else
                {
                    partialResult.push_back("");
                }
            }

            promise->set_value(move(partialResult));
        });
    }

    // Junta os resultados
    vector<string> hoursColumn;
    //hoursColumn.reserve(dataSize); // pode ser útil para o desempenho
    for (auto& f : futures)
    {
        vector<string> partial = f.get();
        hoursColumn.insert(hoursColumn.end(), partial.begin(), partial.end());
    }

    // Pegando tipo/nome da coluna original
    string typeColumn = df.getColumnType(idxColumn);
    string nameColumn = df.getColumnName(idxColumn);

    // Novo DataFrame com a coluna hour
    vector<string> colNames = {nameColumn};
    vector<string> colTypes = {typeColumn};

    vector<ElementType> colHour;
    for (size_t i=0; i<hoursColumn.size(); i++)
    {
        const auto& hour = hoursColumn[i];
        colHour.push_back(hour);
    }

    DataFrame dfHours(colNames, colTypes);
    dfHours.addColumn(colHour, nameColumn, typeColumn);

    return dfHours;
}

DataFrame classify_accounts_parallel(DataFrame& df, const string& id, const string& class_first, const string& class_sec, ThreadPool& tp) {
    // Toma os índices das colunas relevantes
    int id_idx    = df.getColumnIndex(id);
    int media_idx = df.getColumnIndex(class_first);
    int saldo_idx = df.getColumnIndex(class_sec);

    // Busca as colunas relevantes
    auto id_col    = df.getColumn(id_idx);
    auto media_col = df.getColumn(media_idx);
    auto saldo_col = df.getColumn(saldo_idx);

    int numRecords = df.getNumRecords();
    int num_threads = tp.size();
    int block_size = (numRecords + num_threads - 1) / num_threads;

    // Promises comunicação de threads
    vector<future<vector<ElementType>>> futures_ids;
    vector<future<vector<ElementType>>> futures_categorias;

    // Para cada thread
    for (int t = 0; t < num_threads; ++t) {
        // Define os limites do bloco
        int start = t * block_size;
        int end = min(start + block_size, numRecords);
        
        // Define vetor de promessas para clientes e suas categorias
        promise<vector<ElementType>> p_ids;
        promise<vector<ElementType>> p_categorias;
        futures_ids.push_back(p_ids.get_future());
        futures_categorias.push_back(p_categorias.get_future());

        // Cada thread processa uma lambda, que processa um bloco
        tp.enqueue(
            [start, end, &id_col, &media_col, &saldo_col, p_ids = move(p_ids), p_categorias = move(p_categorias)]() mutable {
                // Cria um vetor local pra armazenar resultados do bloco
                vector<ElementType> ids, categorias;
    
                // Para cada entrada do blobo
                for (int i = start; i < end; ++i) {
                    // Verifica se os valores estão no formato certo
                    if (!holds_alternative<int>(id_col[i]) || 
                        !holds_alternative<float>(media_col[i]) || 
                        !holds_alternative<float>(saldo_col[i]))
                        continue;
                    
                    // Se está tudo certo, extrai os valores 
                    int account_id = get<int>(id_col[i]);
                    float media = get<float>(media_col[i]);
                    float saldo = get<float>(saldo_col[i]);
    
                    // Classifica pessoas de acordo com as médias
                    string categoria;
                    if (media > 500) {
                        categoria = "A";
                    } else if (media >= 200 && media <= 500) {
                        categoria = (saldo > 10000) ? "B" : "C";
                    } else {
                        categoria = "D";
                    }
    
                    // E armazena os resultados
                    ids.push_back(account_id);
                    categorias.push_back(categoria);
                }
    
                p_ids.set_value(move(ids));
                p_categorias.set_value(move(categorias));
            }
        );
    }

    // Espera todas as threads e junta os resultados
    vector<string> colNames = {"account_id", "categoria"};
    vector<string> colTypes = {"int", "string"};
    DataFrame result(colNames, colTypes);

    // Para cada thread
    for (int t = 0; t < num_threads; ++t) {
        // Pegue os resultados dos futuros
        auto ids = futures_ids[t].get();
        auto categorias = futures_categorias[t].get();

        for (size_t i = 0; i < ids.size(); ++i) {
            result.columns[0].push_back(ids[i]);
            result.columns[1].push_back(categorias[i]);
            result.numRecords++;
        }
    }

    return result;
}

DataFrame sort_by_column_parallel(const DataFrame& df, const string& key_col, ThreadPool& pool, bool ascending) {
	size_t key_idx = df.getColumnIndex(key_col);
	const auto& key_column = df.columns[key_idx];
	size_t n = df.getNumRecords();
	vector<size_t> indices(n);
	iota(indices.begin(), indices.end(), 0);

	int num_threads = pool.size();
	size_t block_size = (n + num_threads - 1) / num_threads;
	vector<vector<size_t>> sorted_blocks(num_threads);
	vector<future<void>> futures;

	for (int i = 0; i < num_threads; ++i) {
		size_t start = i * block_size;
		size_t end = min(start + block_size, n);
		vector<future<void>> futures;

		for (int i = 0; i < num_threads; ++i) {
			size_t start = i * block_size;
			size_t end = min(start + block_size, n);
            // faz a ordenação
			futures.push_back(async(std::launch::async, [&, start, end, i]() {
				vector<size_t> local(indices.begin() + start, indices.begin() + end);
				sort(local.begin(), local.end(), [&](size_t a, size_t b) {
                    if (holds_alternative<int>(key_column[a]) && holds_alternative<int>(key_column[b])) {
                        return ascending ? get<int>(key_column[a]) < get<int>(key_column[b]) 
                                         : get<int>(key_column[a]) > get<int>(key_column[b]);
                    }
                    if (holds_alternative<float>(key_column[a]) && holds_alternative<float>(key_column[b])) {
                        return ascending ? get<float>(key_column[a]) < get<float>(key_column[b]) 
                                         : get<float>(key_column[a]) > get<float>(key_column[b]);
                    }
                    return false;
                });
				sorted_blocks[i] = move(local);
			}));
		}

	}

	for (auto& f : futures) f.get();

	// merge com heap
	auto cmp = [&](pair<size_t, int> a, pair<size_t, int> b) {
        float fvalA = holds_alternative<int>(key_column[a.first]) ? static_cast<float>(get<int>(key_column[a.first])) : get<float>(key_column[a.first]);
        float fvalB = holds_alternative<int>(key_column[b.first]) ? static_cast<float>(get<int>(key_column[b.first])) : get<float>(key_column[b.first]);
        return ascending ? fvalA > fvalB : fvalA < fvalB;
    };
	priority_queue<pair<size_t, int>, vector<pair<size_t, int>>, decltype(cmp)> pq(cmp);
	vector<size_t> pos(num_threads, 0);

	for (int i = 0; i < num_threads; ++i)
		if (!sorted_blocks[i].empty())
			pq.emplace(sorted_blocks[i][0], i);

	vector<size_t> final_indices;
	while (!pq.empty()) {
		auto [idx, block] = pq.top();
		pq.pop();
		final_indices.push_back(idx);
		if (++pos[block] < sorted_blocks[block].size())
			pq.emplace(sorted_blocks[block][pos[block]], block);
	}

	// Construir metadados do DataFrame resultante
    vector<string> result_col_names;
    vector<string> result_col_types;

    for (const auto& name : df.colNames) {
        result_col_names.push_back(name);
        result_col_types.push_back(df.colTypes.at(name));
    }
	DataFrame result(result_col_names, result_col_types);
	for (size_t i : final_indices) {
		vector<string> record;
		for (size_t j = 0; j < df.getNumCols(); ++j) {
			const auto& val = df.columns[j][i];
			if (holds_alternative<int>(val)) {
				record.push_back(to_string(get<int>(val)));
			} else if (holds_alternative<float>(val)) {
				record.push_back(to_string(get<float>(val)));
			} else if (holds_alternative<bool>(val)) {
				record.push_back(get<bool>(val) ? "true" : "false");
			} else if (holds_alternative<string>(val)) {
				record.push_back(get<string>(val));
			}
		}
		result.addRecord(record);
	}
	return result;
}

unordered_map<string, ElementType> getQuantiles(const DataFrame& df, const string& colName, const vector<double>& quantiles, ThreadPool& pool) {
    unordered_map<string, ElementType> result;

    DataFrame sorted_df = sort_by_column_parallel(df, colName, pool, true);
    const auto& sorted_col = sorted_df.getColumn(sorted_df.getColumnIndex(colName));
    int n = sorted_df.getNumRecords();

    // função para calcular os quantis
    auto get_quantile_value = [&](double quantile) -> ElementType {
        if (n == 0) return ElementType{};
        double pos = quantile * (n - 1);
        int lower = floor(pos);
        int upper = ceil(pos);
        if (lower == upper) {
            return sorted_col[lower];
        } 
        // se for um numero par de records calcula a media
        else {
            if (holds_alternative<int>(sorted_col[lower]) && holds_alternative<int>(sorted_col[upper])) {
                double lower_val = get<int>(sorted_col[lower]);
                double upper_val = get<int>(sorted_col[upper]);
                return static_cast<int>((upper - pos) * lower_val + (pos - lower) * upper_val);
            } else if (holds_alternative<float>(sorted_col[lower]) && holds_alternative<float>(sorted_col[upper])) {
                float lower_val = get<float>(sorted_col[lower]);
                float upper_val = get<float>(sorted_col[upper]);
                return static_cast<float>((upper - pos) * lower_val + (pos - lower) * upper_val);
            } else {
                return sorted_col[lower];
            }
        }
    };
    // adiciona os quantis no df
    for (double q : quantiles) {
        string label;
        if (q == 0.0) label = "min";
        else if (q == 1.0) label = "max";
        else if (q == 0.5) label = "median";
        else label = "Q" + to_string(static_cast<int>(q * 100));
        result[label] = get_quantile_value(q);
    }

    return result;
}

double calculateMeanParallel(const DataFrame& df, const string& target_col, ThreadPool& pool) {
    // Obtém o índice da coluna de interesse
    int target_idx = df.getColumnIndex(target_col);
    const auto& target_vec = df.columns[target_idx];

    int total_records = df.getNumRecords();
    int num_threads = pool.size();
    int block_size = (total_records + num_threads - 1) / num_threads;

    // Cria promessas e futuros
    vector<promise<pair<double, int>>> promises(num_threads);
    vector<future<pair<double, int>>> futures;

    // Relacionamento entre promessas e futuros
    for (auto& p : promises) {
        futures.push_back(p.get_future());
    }

    // Divide o trabalho entre as threads
    for (int t = 0; t < num_threads; ++t) {
        int start = t * block_size;
        int end = min(start + block_size, total_records);

        pool.enqueue([&, start, end, t]() mutable {
            double local_sum = 0.0;
            int count = 0;

            for (int i = start; i < end; ++i) {
                if (holds_alternative<int>(target_vec[i])) {
                    local_sum += get<int>(target_vec[i]);
                    count++;
                } else if (holds_alternative<float>(target_vec[i])) {
                    local_sum += get<float>(target_vec[i]);
                    count++;
                }
            }

            // Define o resultado local da thread
            promises[t].set_value(make_pair(local_sum, count));
        });
    }

    // Fusão dos resultados
    double total_sum = 0.0;
    int total_count = 0;

    for (auto& f : futures) {
        auto result = f.get();
        total_sum += result.first;
        total_count += result.second;
    }

    // Calcula a média
    return total_count > 0 ? total_sum / total_count : 0.0;
}

DataFrame summaryStats(const DataFrame& df, const string& colName, ThreadPool& pool) {
    // Quantis que queremos calcular
    vector<double> quantilesToCompute = {0.0, 0.25, 0.5, 0.75, 1.0};
    unordered_map<string, ElementType> quantile_results = getQuantiles(df, colName, quantilesToCompute, pool);
    
    for (auto& [label, val] : quantile_results) {
        if (holds_alternative<int>(val)) {
            val = static_cast<float>(get<int>(val));
        }
    }

    // Calcula a média
    double mean = calculateMeanParallel(df, colName, pool);

    // Monta os nomes e tipos do DataFrame de saída
    vector<string> colNames = {"statistic", "value"};
    vector<string> colTypes = {"string", "float"};
    DataFrame summary_df(colNames, colTypes);

    // Adiciona os quantis no DataFrame
    summary_df.addRecord({"min", to_string(get<float>(quantile_results["min"]))});
    summary_df.addRecord({"Q1", to_string(get<float>(quantile_results["Q25"]))});
    summary_df.addRecord({"median", to_string(get<float>(quantile_results["median"]))});
    summary_df.addRecord({"Q3", to_string(get<float>(quantile_results["Q75"]))});
    summary_df.addRecord({"max", to_string(get<float>(quantile_results["max"]))});

    // Adiciona a média
    summary_df.addRecord({"mean", to_string(mean)});

    return summary_df;
}

DataFrame top_10_cidades_transacoes(const DataFrame& df, const string& colName, ThreadPool& pool) {
    // Conta o número de transações por cidade
    DataFrame contagem = count_values(df, colName, pool);

    // Ordena de forma decrescente pelo número de transações
    DataFrame ordenado = sort_by_column_parallel(contagem, contagem.getColumnName(1), pool, false);

    // Monta os nomes e tipos do DataFrame de saída
    vector<string> colNames = {"location", "num_trans"};
    vector<string> colTypes = {"string", "int"};
    DataFrame top_10(colNames, colTypes);

    // Adiciona as 10 primeiras cidades no df
    size_t max_records = 10;
    for (size_t i = 0; i < max_records; ++i) {
        vector<ElementType> record = ordenado.getRecord(i);
        string cidade = std::get<string>(record[0]);
        int num_trans = std::get<int>(record[1]);

        top_10.addRecord({cidade, to_string(num_trans)});
    }
    
    return top_10;
}

// DataFrame abnormal_transactions(const DataFrame& df, const string& transactionIDCol, const string& amountCol, const string& locationCol, const string& accountCol, ThreadPool& pool)
// {
//     // Obtendo os Quantis da coluna amount
//     unordered_map<string, ElementType> quantiles = getQuantiles(df, amountCol, {0.25, 0.75}, pool);
//     float q1 = get<float>(quantiles["Q25"]);
//     float q3 = get<float>(quantiles["Q75"]);
//     float iqr = q3 - q1;

//     // Limites
//     float lower = (q1 - 1.5f * iqr < 0) ? 1.0f : (q1 - 1.5f * iqr);
//     float upper = q3 + 1.5f * iqr;

//     //cout << "Q1: " << q1 << ", Q3: " << q3 << ", Lower: " << lower << ", Upper: " << upper << endl;

//     int idxTrans = df.getColumnIndex(transactionIDCol);
//     int idxAmount = df.getColumnIndex(amountCol);
//     int idxLocation = df.getColumnIndex(locationCol);
//     int idxAccount = df.getColumnIndex(accountCol);

//     vector<ElementType> ids;
//     vector<ElementType> suspiciousLocation;
//     vector<ElementType> suspiciousAmount;

//     size_t dataSize = df.getNumRecords();
//     int numThreads = pool.size();
//     size_t blockSize = (dataSize + numThreads - 1) / numThreads;
//     vector<future<tuple<vector<ElementType>, vector<ElementType>, vector<ElementType>>>> futures;

//     // Encontrando anormalidades
//     for (int i=0; i<df.getNumRecords(); i++)
//     {
//         float amount = get<float>(df.getColumn(idxAmount)[i]);
//         string location = get<string>(df.getColumn(idxLocation)[i]);
//         int id = get<int>(df.getColumn(idxTrans)[i]);
//         int accountID = get<int>(df.getColumn(idxAccount)[i]);

//         bool isAmountSus = (amount < lower || amount > upper);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
//         bool isLocationSus = false;
//         auto& seenLocations = accountSeenLocations[accountID];

//         if (seenLocations.find(location) == seenLocations.end()) {
//             if (!seenLocations.empty()) {
//                 // Localização nova para essa conta
//                 isLocationSus = true;
//             }
//             seenLocations.insert(location);
//         }

//         if (isAmountSus == true || isLocationSus == true)
//         {
//             ids.push_back(id);
//             suspiciousLocation.push_back(isLocationSus);
//             suspiciousAmount.push_back(isAmountSus);
//         }
//     }

//     // DataFrame Resultado
//     string typeColumn = df.getColumnType(idxTrans);
//     vector<string> colNames = {transactionIDCol, "is_location_suspicious", "is_amount_suspicious"};
//     vector<string> colTypes = {typeColumn, "bool", "bool"};

//     DataFrame result(colNames, colTypes);
//     result.addColumn(ids, transactionIDCol, typeColumn);
//     result.addColumn(suspiciousLocation, "is_location_suspicious", "bool");
//     result.addColumn(suspiciousAmount, "is_amount_suspicious", "bool");

//     return result;
// }

DataFrame abnormal_transactions(const DataFrame& df, const string& transactionIDCol, const string& amountCol, const string& locationCol, const string& accountCol, ThreadPool& pool)
{
    unordered_map<string, ElementType> quantiles = getQuantiles(df, amountCol, {0.25, 0.75}, pool);
    float q1 = get<float>(quantiles["Q25"]);
    float q3 = get<float>(quantiles["Q75"]);
    float iqr = q3 - q1;

    float lower = (q1 - 0.45f * iqr < 0) ? 1.0f : (q1 - 0.45f * iqr);
    float upper = q3 + 1.25f * iqr;

    //cout << "Q1: " << q1 << ", Q3: " << q3 << ", Lower: " << lower << ", Upper: " << upper << endl;

    size_t dataSize = df.getNumRecords();
    int numThreads = pool.size();
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;

    // cout << "Data Size: " << dataSize << endl;
    // cout << "Num Threads: " << numThreads << endl;
    // cout << "Block Size: " << blockSize << endl;

    int idxTrans = df.getColumnIndex(transactionIDCol);
    int idxAmount = df.getColumnIndex(amountCol);
    int idxLocation = df.getColumnIndex(locationCol);
    int idxAccount = df.getColumnIndex(accountCol);

    const vector<ElementType>& colTrans = df.getColumn(idxTrans);
    const vector<ElementType>& colAmount = df.getColumn(idxAmount);
    const vector<ElementType>& colLocation = df.getColumn(idxLocation);
    const vector<ElementType>& colAccount = df.getColumn(idxAccount);

    vector<future<tuple<vector<ElementType>, vector<ElementType>, vector<ElementType>>>> futures;

    for (int t = 0; t < numThreads; ++t)
    {
        size_t start = t * blockSize;
        size_t end = std::min(start + blockSize, dataSize);
        if (start >= end) break;

        auto promise = make_shared<std::promise<tuple<vector<ElementType>, vector<ElementType>, vector<ElementType>>>>();
        futures.push_back(promise->get_future());

        pool.enqueue([start, end, lower, upper,
                    &colTrans, &colAmount, &colLocation, &colAccount, promise]() {
            vector<ElementType> ids;
            vector<ElementType> suspiciousLocation;
            vector<ElementType> suspiciousAmount;
            unordered_map<int, unordered_set<string>> accountSeenLocations;

            for (size_t i = start; i < end; i++) {
                float amount = get<float>(colAmount[i]);
                string location = get<string>(colLocation[i]);
                int id = get<int>(colTrans[i]);
                int accountID = get<int>(colAccount[i]);

                bool isAmountSus = (amount < lower || amount > upper);
                bool isLocationSus = false;

                auto& seenLocations = accountSeenLocations[accountID];
                if (seenLocations.find(location) == seenLocations.end()) {
                    if (!seenLocations.empty()) {
                        // Nova localidade para a conta
                        isLocationSus = true;
                    }
                    seenLocations.insert(location);
                }

                if (isAmountSus || isLocationSus) {
                    ids.push_back(id);
                    suspiciousLocation.push_back(isLocationSus);
                    suspiciousAmount.push_back(isAmountSus);
                }
            }

            promise->set_value(make_tuple(move(ids), move(suspiciousLocation), move(suspiciousAmount)));
        });
    }


    // Juntando resultados das threads
    vector<ElementType> ids, suspiciousLocation, suspiciousAmount;
    for (auto& fut : futures)
    {
        auto [localIds, localLoc, localAmt] = fut.get();
        ids.insert(ids.end(), localIds.begin(), localIds.end());
        suspiciousLocation.insert(suspiciousLocation.end(), localLoc.begin(), localLoc.end());
        suspiciousAmount.insert(suspiciousAmount.end(), localAmt.begin(), localAmt.end());
    }

    // Monta o DataFrame
    string typeColumn = df.getColumnType(idxTrans);
    vector<string> colNames = {transactionIDCol, "is_location_suspicious", "is_amount_suspicious"};
    vector<string> colTypes = {typeColumn, "bool", "bool"};

    DataFrame result(colNames, colTypes);
    result.addColumn(ids, transactionIDCol, typeColumn);
    result.addColumn(suspiciousLocation, "is_location_suspicious", "bool");
    result.addColumn(suspiciousAmount, "is_amount_suspicious", "bool");

    return result;
}
