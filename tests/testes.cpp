#include <iostream>
#include "../include/df.h"
#include "../include/tratadores.h"
#include "../include/csv_extractor.h"
#include "../include/threads.h"

using namespace std;

mutex cout_mutex;

int main() {
    const int NUM_THREADS = thread::hardware_concurrency();
    ThreadPool pool(NUM_THREADS);

    vector<string> colNames = {"ID", "Nome", "Salario"};
    vector<string> colTypes = {"int", "string", "float"};
    DataFrame df(colNames, colTypes);

    df.addRecord({"1", "Camacho", "5000.5"});
    df.addRecord({"2", "Bebel", "6200.0"});
    df.addRecord({"3", "Yuri", "4700.75"});

    cout << "\nDataFrame original:\n";
    df.printDF();

    vector<ElementType> isHighSalary = {true, true, false};
    string newColName = "Rico?";
    string newColType = "bool";
    df.addColumn(isHighSalary, newColName, newColType);

    cout << "\nDataFrame com coluna bool adicionada:\n";
    df.printDF();

    cout << "\n--- Metadados do DataFrame ---\n";
    cout << "Num de registros: " << df.numRecords << endl;
    cout << "Num de colunas: " << df.numCols << endl;

    cout << "\nNomes das colunas:\n";
    for (const auto& name : df.colNames)
        cout << "- " << name << endl;

    cout << "\nIdx das colunas:\n";
    for (const auto& [name, idx] : df.idxColumns)
        cout << "- " << name << ": " << idx << endl;

    cout << "\nTipos das colunas:\n";
    for (const auto& [name, type] : df.colTypes)
        cout << "- " << name << ": " << type << endl;

    cout << "\nTESTE PARA FILTRAR REGISTROS:\n";
    auto cond = [&](const vector<ElementType>& row) -> bool {
        return get<float>(row[2]) > 5000.0f;
    };

    DataFrame filtrado = filter_records(df, cond, NUM_THREADS, pool);

    cout << "\nDataFrame filtrado (salario > 5000):\n";
    filtrado.printDF();

    cout << "\nTeste para o groupby\n";
    vector<string> colNames_2 = {"account_id", "amount"};
    vector<string> colTypes_2 = {"int", "float"};
    DataFrame df_2(colNames_2, colTypes_2);

    df_2.addRecord({"1", "100.0"});
    df_2.addRecord({"2", "200.0"});
    df_2.addRecord({"1", "300.0"});
    df_2.addRecord({"2", "400.0"});
    df_2.addRecord({"3", "150.0"});

    DataFrame df_grouped = groupby_mean(df_2, "account_id", "amount", pool);
    df_grouped.printDF();

    vector<string> colNames_3 = {"id", "nome"};
    vector<string> colTypes_3 = {"int", "string"};
    DataFrame df_3(colNames_3, colTypes_3);
    df_3.addRecord({"1", "Alice"});
    df_3.addRecord({"2", "Bob"});
    df_3.addRecord({"3", "Carlos"});

    vector<string> colNames_4 = {"id", "idade"};
    vector<string> colTypes_4 = {"int", "int"};
    DataFrame df_4(colNames_4, colTypes_4);
    df_4.addRecord({"1", "23"});
    df_4.addRecord({"2", "30"});
    df_4.addRecord({"4", "40"});

    DataFrame df_result = join_by_key(df_3, df_4, "id", pool);

    cout << "Resultado do join:" << endl;
    df_result.printDF();

    // cout << "\nTeste para transactions.csv\n";
    // DataFrame* df_teste = readCSV("data/tests/teste.csv", pool);

    // cout << "\nTeste groupby_mean para transactions.csv\n";
    // DataFrame df_grouped_t = groupby_mean(*df_teste, "account_id", "amount", pool);
    // df_grouped_t.printDF();

    // delete df_teste;

    vector<string> colName_ = {"time_start"};
    vector<string> colType_ = {"string"};
    DataFrame df_5(colName_, colType_);

    df_5.addRecord({"01:46:23"});
    df_5.addRecord({"02:35:22"});
    df_5.addRecord({"01:45:06"});
    df_5.addRecord({"02:18:33"});
    df_5.addRecord({"03:23:59"});
    df_5.addRecord({"02:44:33"});
    df_5.addRecord({"02:56:00"});

    cout << "\nResultado do get_hour_by_time:" << endl;
    DataFrame df_hour = get_hour_by_time(df_5, "time_start", pool);
    df_hour.printDF();

    cout << "\nResultado do count_values:" << endl;
    DataFrame df_count = count_values(df_hour, "time_start", pool);
    df_count.printDF();

    ///

    vector<string> colName__ = {"transaction_id", "account_id", "amount", "location"};
    vector<string> colType__ = {"int", "int", "float", "string"};
    DataFrame df_6(colName__, colType__);

    df_6.addRecord({"1", "10", "10", "Guzmantown"});
    df_6.addRecord({"2", "20", "200", "Josephfort"});
    df_6.addRecord({"3", "10", "500", "Guzmantown"});
    df_6.addRecord({"4", "20", "700", "Josephfort"});
    df_6.addRecord({"5", "20", "200", "Marybury"});
    df_6.addRecord({"6", "30", "500", "Marybury"});
    df_6.addRecord({"7", "30", "20000", "Marybury"});
    df_6.addRecord({"8", "20", "600", "Hartberg"});
    df_6.addRecord({"9", "20", "100", "Hartberg"});

    df_6.addRecord({"10", "10", "10", "Guzmantown"});
    df_6.addRecord({"11", "20", "200", "Josephfort"});
    df_6.addRecord({"12", "10", "500", "Guzmantown"});
    df_6.addRecord({"13", "20", "700", "Josephfort"});
    df_6.addRecord({"14", "20", "200", "Marybury"});
    df_6.addRecord({"15", "30", "500", "Marybury"});
    df_6.addRecord({"16", "30", "20000", "Marybury"});
    df_6.addRecord({"17", "20", "600", "Hartberg"});
    df_6.addRecord({"18", "20", "100", "Hartberg"});

    df_6.addRecord({"19", "10", "10", "Guzmantown"});
    df_6.addRecord({"20", "20", "200", "Josephfort"});
    df_6.addRecord({"21", "10", "500", "Guzmantown"});
    df_6.addRecord({"22", "20", "700", "Josephfort"});
    df_6.addRecord({"23", "20", "200", "Marybury"});
    df_6.addRecord({"24", "30", "500", "Marybury"});
    df_6.addRecord({"25", "30", "20000", "Marybury"});
    df_6.addRecord({"26", "20", "600", "Hartberg"});
    df_6.addRecord({"27", "20", "100", "Hartberg"});

    df_6.addRecord({"28", "10", "10", "Guzmantown"});
    df_6.addRecord({"29", "20", "200", "Josephfort"});
    df_6.addRecord({"30", "10", "500", "Guzmantown"});
    df_6.addRecord({"31", "20", "700", "Josephfort"});
    df_6.addRecord({"32", "20", "200", "Marybury"});
    df_6.addRecord({"33", "30", "500", "Marybury"});
    df_6.addRecord({"34", "30", "20000", "Marybury"});
    df_6.addRecord({"35", "20", "600", "Hartberg"});
    df_6.addRecord({"36", "20", "100", "Hartberg"});

    df_6.addRecord({"37", "10", "10", "Guzmantown"});
    df_6.addRecord({"38", "20", "200", "Josephfort"});
    df_6.addRecord({"39", "10", "500", "Guzmantown"});
    df_6.addRecord({"40", "20", "700", "Josephfort"});
    df_6.addRecord({"41", "20", "200", "Marybury"});
    df_6.addRecord({"42", "30", "500", "Marybury"});
    df_6.addRecord({"43", "30", "20000", "Marybury"});
    df_6.addRecord({"44", "20", "600", "Hartberg"});
    df_6.addRecord({"45", "20", "100", "Hartberg"});

    df_6.addRecord({"46", "10", "10", "Guzmantown"});
    df_6.addRecord({"47", "20", "200", "Josephfort"});
    df_6.addRecord({"48", "10", "500", "Guzmantown"});
    df_6.addRecord({"49", "20", "700", "Josephfort"});
    df_6.addRecord({"50", "20", "200", "Marybury"});
    df_6.addRecord({"51", "30", "500", "Marybury"});
    df_6.addRecord({"52", "30", "20000", "Marybury"});
    df_6.addRecord({"53", "20", "600", "Hartberg"});
    df_6.addRecord({"54", "20", "100", "Hartberg"});

    df_6.addRecord({"55", "10", "10", "Guzmantown"});
    df_6.addRecord({"56", "20", "200", "Josephfort"});
    df_6.addRecord({"57", "10", "500", "Guzmantown"});
    df_6.addRecord({"58", "20", "700", "Josephfort"});
    df_6.addRecord({"59", "20", "200", "Marybury"});
    df_6.addRecord({"60", "30", "500", "Marybury"});
    df_6.addRecord({"61", "30", "20000", "Marybury"});
    df_6.addRecord({"62", "20", "600", "Hartberg"});
    df_6.addRecord({"63", "20", "100", "Hartberg"});

    df_6.addRecord({"64", "10", "10", "Guzmantown"});
    df_6.addRecord({"65", "20", "200", "Josephfort"});
    df_6.addRecord({"66", "10", "500", "Guzmantown"});
    df_6.addRecord({"67", "20", "700", "Josephfort"});
    df_6.addRecord({"68", "20", "200", "Marybury"});
    df_6.addRecord({"69", "30", "500", "Marybury"});
    df_6.addRecord({"70", "30", "20000", "Marybury"});
    df_6.addRecord({"71", "20", "600", "Hartberg"});
    df_6.addRecord({"72", "20", "100", "Hartberg"});

    df_6.addRecord({"73", "10", "10", "Guzmantown"});
    df_6.addRecord({"74", "20", "200", "Josephfort"});
    df_6.addRecord({"75", "10", "500", "Guzmantown"});
    df_6.addRecord({"76", "20", "700", "Josephfort"});
    df_6.addRecord({"77", "20", "200", "Marybury"});
    df_6.addRecord({"78", "30", "500", "Marybury"});
    df_6.addRecord({"79", "30", "20000", "Marybury"});
    df_6.addRecord({"80", "20", "600", "Hartberg"});
    df_6.addRecord({"81", "20", "100", "Hartberg"});

    cout << "\nResultado do abnormal_transactions:" << endl;
    DataFrame df_abnormal = abnormal_transactions(df_6, "transaction_id", "amount", "location", "account_id", pool);
    df_abnormal.printDF();

    return 0;
}
