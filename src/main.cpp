#include "../include/json.hpp"
#include "../include/list.h"

struct DataBase {
    string nameBD; // название БД
    int tupleslimit; // лимит строк
    SinglyLinkedList<string> nametables; // названия таблиц
    SinglyLinkedList<string> stlb; // столбцы таблиц
    SinglyLinkedList<int> fileindex; // кол-во файлов таблиц
    SinglyLinkedList<int> countlines; // кол-во строк таблиц

    void parse() { // ф-ия парсинга
        nlohmann::json objJson;
        ifstream fileinput;
        fileinput.open("../schema.json");
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
                nametables.push_back(elem.key());
                
                string kolonki = elem.key() + "_pk_sequence,"; // добавление первичного ключа
                for (auto str : objJson["structure"][elem.key()].items()) {
                    kolonki += str.value();
                    kolonki += ',';
                }
                kolonki.pop_back(); // удаление последней запятой
                stlb.push_back(kolonki);
                fileindex.push_back(1);
                countlines.push_back(1);
            }
        } else {
            cout << "Объект подкаталогов не найден!" << endl;
            exit(0);
        }
    }

    void mkdir() { // ф-ия формирования директории
        string command;
        command = "mkdir ../" + nameBD; // каталог
        system(command.c_str());

        for (int i = 0; i < nametables.size; ++i) { // подкаталоги и файлы в них
            command = "mkdir ../" + nameBD + "/" + nametables.getvalue(i);
            system(command.c_str());
            string filepath = "../" + nameBD + "/" + nametables.getvalue(i) + "/1.csv";
            ofstream file;
            file.open(filepath);
            file << stlb.getvalue(i) << endl;
            file.close();

            // Блокировка таблицы
            filepath = "../" + nameBD + "/" + nametables.getvalue(i) + "/" + nametables.getvalue(i) + "_lock.txt";
            file.open(filepath);
            file << "open";
            file.close();
        }
    }

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

    void insert(string& table, string& values) { // ф-ия вставки в таблицу
        string check, filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        int index = nametables.getindex(table); // получаем индекс таблицы(aka key)
        int val = fileindex.getvalue(index); // получаем номер файла по индеку(ключу)
        fstream file;
        file.open(filepath);
        file >> check;
        if (check == "open") { // проверка, открыта ли таблица
            file << "lock";
            file.close();

            // вставка значений в csv, не забывая про увеличение ключа
            filepath = "../" + nameBD + "/" + table + "/" + to_string(val) + ".csv";
            int countline = CountLine(filepath); // кол-во строк конкретного файла таблицы
            if (countline == tupleslimit) { // если достигнут лимит, то создаем новый файл
                fileindex.replace(index, val++);
                filepath = "../" + nameBD + "/" + table + "/" + to_string(val++) + ".csv";
            }
            file.open(filepath, ios::app);
            val = countlines.getvalue(index);
            values = to_string(val) + "," + values;
            file << values << endl;
            val++;
            countlines.replace(index, val);
            file.close();

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            file.open(filepath);
            file << "open";
            file.close();
            cout << "Команда выполнена!" << endl;
        } else {
            file.close();
            cout << "Доступ закрыт: таблица открыта другим пользователем!";
        }
    }
    
    void isValidInsert() { // ф-ия проверки ввода команды insert
        string command;
        cin >> command;
        if (command == "into") {
            string table;
            cin >> table;
            if (nametables.getindex(table) != -1) {
                cin >> command;
                if (command == "values") {
                    cin.ignore();
                    string values;
                    getline(cin, values);
                    if (values[0] == '(' && values[values.size()-1] == ')') {
                        values.erase(values.begin());
                        values.erase(values.end() - 1);
                        insert(table, values);
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
};

int main() {

    DataBase carshop;

    carshop.parse();
    carshop.mkdir();

    while(true) {
        cout << "Введите команду: ";
        string command;
        cin >> command;
        if (command == "insert") {
            carshop.isValidInsert();
        } else if (command == "delete") {

        } else if (command == "select") {

        } else if (command == "exit") {
            break;
        } else {
            cout << "Нет такой команды!" << endl;
        }
        cout << endl;
    }

    return 0;
}