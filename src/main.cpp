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
        int fileid = 1; // номер файла csv
        
        fstream file;
        file.open(filepath);
        file >> check;
        if (check == "open") { // проверка, открыта ли таблица
            file << "lock";
            file.close();

            // вставка значений в csv, не забывая про увеличение ключа
            filepath = "../" + nameBD + "/" + table + "/1.csv";
            int countline = CountLine(filepath); // кол-во строк конкретного файла таблицы
            while (true) {
                if (countline == tupleslimit) { // если достигнут лимит, то создаем/открываем другой файл
                    fileid++;
                    filepath = "../" + nameBD + "/" + table + "/" + to_string(fileid) + ".csv";
                    if (fileindex.getvalue(index) < fileid) {
                        fileindex.replace(index, fileid);
                    }
                } else break;
                countline = CountLine(filepath);
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

    void del(string& table, string&stolbec, string& values) {
        string check, filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        int index = nametables.getindex(table);
        ifstream fileinput;
        ofstream fileoutput;
        fileinput.open(filepath);
        fileinput >> check;
        if (check == "open") { // проверка, открыта ли таблица
            fileinput.close();
            fileoutput.open(filepath);
            fileoutput << "lock";
            fileoutput.close();

            // нахождение индекса столбца в файле
            string str = stlb.getvalue(index);
            stringstream ss(str);
            int stolbecindex = 1;
            while (getline(ss, str, ',')) {
                if (str == stolbec) break;
                stolbecindex++;
            }

            // операция удаления по всем файлам csv
            int copy = fileindex.getvalue(index);
            while (copy != 0) {
                filepath = "../" + nameBD + "/" + table + "/" + to_string(copy) + ".csv";
                fileinput.open(filepath);
                string filteredlines;
                str.clear();
                while (getline(fileinput, str)) {
                    stringstream iss(str);
                    string line;
                    int currentIndex = 1;
                    bool shouldRemove = false;
                    while(getline(iss, line, ',')) {
                        if (currentIndex == stolbecindex && line == values) {
                            shouldRemove = true;
                            break;
                        }
                        currentIndex++;
                    }
                    if (!shouldRemove) {
                        filteredlines += str + "\n"; 
                    }
                }
                fileinput.close();

                fileoutput.open(filepath);
                fileoutput << filteredlines;
                fileoutput.close();

                copy--;
            }

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            fileoutput.open(filepath);
            fileoutput << "open";
            fileoutput.close();
            cout << "Команда выполнена!" << endl;
        } else {
            fileinput.close();
            cout << "Доступ закрыт: таблица открыта другим пользователем!";
        }
    }

    void isValidDelete() { // ф-ия проверки ввода команды delete
        string command;
        cin >> command;
        if (command == "from") {
            string table;
            cin >> table;
            int index = nametables.getindex(table);
            if (index != -1) {
                cin >> command;
                if (command == "where") {
                    string stolbec;
                    cin >> stolbec;
                    string str = stlb.getvalue(index);
                    stringstream ss(str);
                    bool check = false;
                    while (getline(ss, str, ',')) {
                        if (str == stolbec) check = true;
                    }
                    if (check) {
                        cin >> command;
                        if (command == "=") {
                            string values;
                            cin >> values;
                            del(table, stolbec, values);
                        } else {
                            cout << "Нарушен синтаксис команды delete!" << endl;
                        }
                    } else {
                        cout << "Нет такой колонки!" << endl;
                    }
                } else {
                    cout << "Нарушен синтаксис команды delete!" << endl;
                }
            } else {
                cout << "Нет такой таблицы!" << endl;
            }
        } else {
            cout << "Нарушен синтаксис команды delete!" << endl;
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
            carshop.isValidDelete();
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