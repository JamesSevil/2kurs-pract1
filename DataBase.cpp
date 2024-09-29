#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include "json.hpp"

using namespace std;

int CountLine(string& filepath) { // ф-ия подсчёта строк в файле
    ifstream file;
    file.open(filepath);
    int countline = 0;
    string line;

    while(getline(file, line)) {
        countline++;
    }
    file.close();

    return countline;
}

void parse(string& nameBD, map<string,vector<string>>& tables, map<string,int>& fileindex, int& tupleslimit, map<string,int>& countlines) { // ф-ия парсинга
    // объект парсинга
    nlohmann::json objJson;
    fstream fileinput;
    fileinput.open("schema.json");
    fileinput >> objJson;
    fileinput.close();

    if (objJson["names"].is_string()) {
       nameBD = objJson["names"]; // Парсим каталог 
    } else {
        cout << "Объект каталога не найден!" << endl;
        exit(0);
    }

    tupleslimit = objJson["tuples_limit"];

    // парсим подкаталоги
    if (objJson.contains("structure") && objJson["structure"].is_object()) { // проверяем, существование объекта и является ли он объектом
        for (auto elem : objJson["structure"].items()) {
            tables[elem.key()] = objJson["structure"][elem.key()]; // таблицы и их столбцы
            fileindex[elem.key()] = 1; // индекс файла csv в таблице
            countlines[elem.key()] = 1; // кол-во строк в таблицах

            // добавляем первичный ключ
            string key = elem.key() + "_pk_sequence";
            tables[elem.key()].insert(tables[elem.key()].begin(), key);
        }
    } else {
        cout << "Объект подкаталогов не найден!" << endl;
        exit(0);
    }
}

void mkdir(string& nameBD, map<string,vector<string>>& tables) { // ф-ия формирования директории
    string command;
    command = "mkdir " + nameBD;
    system(command.c_str());

    for (auto keys : tables) {
        command = "mkdir " + nameBD + "/" + keys.first;
        system(command.c_str());
        string filepath = nameBD + "/" + keys.first + "/1.csv";
        ofstream file;
        file.open(filepath);
        string str; // строка, которую впишем в csv
        for (auto values : keys.second) {
            str += values + ',';
        }
        str.pop_back(); // удаляем последнюю запятую
        file << str << endl;
        file.close();
        str.clear();

        // Блокировка таблицы
        filepath = nameBD + "/" + keys.first + "/" + keys.first + "_lock.txt";
        file.open(filepath);
        file << "open";
        file.close();
    }
}

void insert(string& table, string& values, string& nameBD, map<string,int>& fileindex, int& tupleslimit, map<string,int>& countlines) { // вставка в таблицу
    string check, filepath = nameBD + "/" + table + "/" + table + "_lock.txt";
    fstream file;
    file.open(filepath);
    file >> check;
    if (check == "open") { // проверка, открыта ли таблица
        file << "lock";
        file.close();

        // вставка значений в csv, не забывая про увеличение ключа
        filepath = nameBD + "/" + table + "/" + to_string(fileindex[table]) + ".csv";
        int countline = CountLine(filepath); // кол-во строк конкретного файла таблицы
        if (countline == tupleslimit) { // если достигнут лимит, то создаем новый файл
            fileindex[table]++;
            filepath = nameBD + "/" + table + "/" + to_string(fileindex[table]) + ".csv";
        }
        file.open(filepath, ios::app);
        values = to_string(countlines[table]) + "," + values;
        file << values << endl;
        countlines[table]++;
        file.close();

        filepath = nameBD + "/" + table + "/" + table + "_lock.txt";
        file.open(filepath);
        file << "open";
        file.close();
        cout << "Команда выполнена!" << endl;
    } else {
        file.close();
        cout << "Доступ закрыт: таблица открыта другим пользователем!";
    }
}

void isValidInsert(map<string, vector<string>>& tables, string& nameBD, map<string,int>& fileindex, int& tupleslimit, map<string,int>& countlines) { // ф-ия проверки ввода команды insert
    string command;
    cin >> command;
    if (command == "into") {
        string table;
        cin >> table;
        auto it = tables.find(table);
        if (it != tables.end()) {
            cin >> command;
            if (command == "values") {
                cin.ignore();
                string values;
                getline(cin, values);
                if (values[0] == '(' && values[values.size()-1] == ')') {
                    values.erase(values.begin());
                    values.erase(values.end() - 1);
                    insert(table, values, nameBD, fileindex, tupleslimit, countlines);
                } else {
                    cout << "Нарушен синтаксис команды insert!" << endl;
                }
            } else {
                cout << "Нарушен синтаксис команды insert!" << endl;
            }
        } else {
            cout << "Нет такой таблицы!" << endl;
        }
    } else {
        cout << "Нарушен синтаксис команды insert!" << endl;
    }
}


int main() {

    string nameBD;
    int tupleslimit;
    map<string, vector<string>> tables;
    map<string, int> fileindex;
    map<string, int> countlines;
    parse(nameBD, tables, fileindex, tupleslimit, countlines); // ф-ия парсинга

    mkdir(nameBD, tables); // ф-ия формирования директории

    while(true) {
        cout << "Введите команду: ";
        string command;
        cin >> command;
        if (command == "insert") {
            isValidInsert(tables, nameBD, fileindex, tupleslimit, countlines);
        } else {
            cout << "Нет такой команды!" << endl;
        }
        cout << endl;
    }

    return 0;
}