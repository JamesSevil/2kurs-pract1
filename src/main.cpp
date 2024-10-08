#include "../include/json.hpp"
#include "../include/list.h"

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

string finput(string& filepath) { // чтение из файла
    string result, str;
    ifstream fileinput;
    fileinput.open(filepath);
    while (getline(fileinput, str)) {
        result += str + '\n';
    }
    result.pop_back();
    fileinput.close();
    return result;
}

void foutput(string& filepath, string text) { // запись в файл
    ofstream fileoutput;
    fileoutput.open(filepath);
    fileoutput << text;
    fileoutput.close();
}

struct DataBase {
    string nameBD; // название БД
    int tupleslimit; // лимит строк
    SinglyLinkedList<string> nametables; // названия таблиц
    SinglyLinkedList<string> stlb; // столбцы таблиц
    SinglyLinkedList<int> fileindex; // кол-во файлов таблиц
    SinglyLinkedList<int> countlines; // кол-во строк таблиц

    struct Where { // структура для фильтрации
        string table;
        string column;
        string value;
        string logicalOP;
    };

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

    void checkcommand(string& command) { // ф-ия фильтрации команд
        if (command.substr(0, 11) == "insert into") {
            command.erase(0, 12);
            isValidInsert(command);
        } else if (command.substr(0, 11) == "delete from") {
            command.erase(0, 12);
            isValidDel(command);
        } else if (command.substr(0, 6) == "select") {
            command.erase(0, 7);
            // isValidSelect(command);
        } else if (command == "exit") {
            exit(0);
        } else cout << "Ошибка, неизвестная команда!" << endl; 
    }


    void isValidDel(string& command) { // ф-ия обработки команды DELETE
        string table, conditions;
        int position = command.find_first_of(' ');
        if (position != -1) {
            table = command.substr(0, position);
            conditions = command.substr(position + 1);
        } else table = command;
        if (nametables.getindex(table) != -1) { // проверка таблицы
            // если нет условий, удаляем все
            if (conditions.empty()) {
                del(table);
            } else {
                if (conditions.substr(0, 6) == "where ") { // проверка наличия where
                    conditions.erase(0, 6);

                    // если есть логический оператор
                    if ((conditions.find("or") != -1) || (conditions.find("and") != -1)) {
                        SinglyLinkedList<Where> cond;
                        Where where;
                        position = conditions.find_first_of(' ');
                        if (position != -1) { // проверка синтаксиса
                            where.column = conditions.substr(0, position);
                            conditions.erase(0, position+1);
                            int index = nametables.getindex(table);
                            string str = stlb.getvalue(index);
                            stringstream ss(str);
                            bool check = false;
                            while (getline(ss, str, ',')) if (str == where.column) check = true;
                            if (check) { // проверка столбца
                                if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                    conditions.erase(0, 2);
                                    position = conditions.find_first_of(' ');
                                    if (position != -1) { // проверка синтаксиса
                                        where.value = conditions.substr(0, position);
                                        conditions.erase(0, position+1);
                                        cond.push_back(where);
                                        position = conditions.find_first_of(' ');
                                        if (position != -1) { // проверка синтаксиса
                                            where.logicalOP = conditions.substr(0, position);
                                            conditions.erase(0, position+1);
                                            position = conditions.find_first_of(' ');
                                            if (position != -1) { // проверка синтаксиса
                                                where.column = conditions.substr(0, position);
                                                conditions.erase(0, position+1);
                                                str = stlb.getvalue(index);
                                                stringstream iss(str);
                                                check = false;
                                                while (getline(iss, str, ',')) if (str == where.column) check = true;
                                                if (check) { // проверка столбца
                                                    if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                                        conditions.erase(0, 2);
                                                        position = conditions.find_first_of(' ');
                                                        if (position == -1) {
                                                            where.value = conditions.substr(0);
                                                            cond.push_back(where);
                                                            delWithLogic(cond, table);
                                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                } else cout << "Ошибка, нет такого столбца!" << endl;
                                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                            } else cout << "Ошибка, нет такого столбца!" << endl;
                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    // если нет логического оператор
                    } else {
                        Where where;
                        position = conditions.find_first_of(' ');
                        if (position != -1) { // проверка синтаксиса
                            where.column = conditions.substr(0, position);
                            conditions.erase(0, position+1);
                            int index = nametables.getindex(table);
                            string str = stlb.getvalue(index);
                            stringstream ss(str);
                            bool check = false;
                            while (getline(ss, str, ',')) if (str == where.column) check = true;
                            if (check) { // проверка столбца
                                if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                    conditions.erase(0, 2);
                                    position = conditions.find_first_of(' ');
                                    if (position == -1) { // проверка синтаксиса
                                        where.value = conditions.substr(0);
                                        delWithValue(table, where.column, where.value);
                                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                            } else cout << "Ошибка, нет такого столбца!" << endl;
                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    }

                } else cout << "Ошибка, нарушен синтаксис команды!";
            }
        } else cout << "Ошибка, нет такой таблицы!" << endl;
    }

    void del(string& table) { // ф-ия удаления всех строк таблицы
        string filepath;
        int index = nametables.getindex(table);
        if (checkLockTable(table)) {
            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "close");
            
            // очищаем все файлы
            int copy = fileindex.getvalue(index);
            while (copy != 0) {
                filepath = "../" + nameBD + "/" + table + "/" + to_string(copy) + ".csv";
                foutput(filepath, "");
                copy--;
            }

            foutput(filepath, stlb.getvalue(index)+"\n"); // добавляем столбцы в 1.csv

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }

    void delWithValue(string& table, string& stolbec, string& values) { // ф-ия удаления строк таблицы по значению
        string filepath;
        int index = nametables.getindex(table);
        if (checkLockTable(table)) {
            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "close");

            // нахождение индекса столбца в файле
            string str = stlb.getvalue(index);
            stringstream ss(str);
            int stolbecindex = 0;
            while (getline(ss, str, ',')) {
                if (str == stolbec) break;
                stolbecindex++;
            }

            // удаление строк
            int copy = fileindex.getvalue(index);
            while (copy != 0) {
                filepath = "../" + nameBD + "/" + table + "/" + to_string(copy) + ".csv";
                string text = finput(filepath);
                stringstream stroka(text);
                string filteredlines;
                while (getline(stroka, text)) {
                    stringstream iss(text);
                    string token;
                    int currentIndex = 0;
                    bool shouldRemove = false;
                    while (getline(iss, token, ',')) {
                        if (currentIndex == stolbecindex && token == values) {
                            shouldRemove = true;
                            break;
                        }
                        currentIndex++;
                    }
                    if (!shouldRemove) filteredlines += text + "\n"; 
                }
                foutput(filepath, filteredlines);
                copy--;
            }

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }

    void delWithLogic(SinglyLinkedList<Where>& conditions, string& table) { // ф-ия удаления строк таблицы с логикой
        string filepath;
        int index = nametables.getindex(table);
        if (checkLockTable(table)) {
            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "close");

            // нахождение индекса столбцов в файле
            SinglyLinkedList<int> stlbindex;
            for (int i = 0; i < conditions.size; ++i) {
                string str = stlb.getvalue(index);
                stringstream ss(str);
                int stolbecindex = 0;
                while (getline(ss, str, ',')) {
                    if (str == conditions.getvalue(i).column) {
                        stlbindex.push_back(stolbecindex);
                        break;
                    }
                    stolbecindex++;
                }
            }

            // удаление строк
            int copy = fileindex.getvalue(index);
            while (copy != 0) {
                filepath = "../" + nameBD + "/" + table + "/" + to_string(copy) + ".csv";
                string text = finput(filepath);
                stringstream stroka(text);
                string filteredRows;
                while (getline(stroka, text)) {
                    SinglyLinkedList<bool> shouldRemove;
                    for (int i = 0; i < stlbindex.size; ++i) {
                        stringstream iss(text);
                        string token;
                        int currentIndex = 0;
                        bool check = false;
                        while (getline(iss, token, ',')) { 
                            if (currentIndex == stlbindex.getvalue(i) && token == conditions.getvalue(i).value) {
                                check = true;
                                break;
                            }
                            currentIndex++;
                        }
                        if (check) shouldRemove.push_back(true);
                        else shouldRemove.push_back(false);
                    }
                    if (conditions.getvalue(1).logicalOP == "and") { // Если оператор И
                        if (shouldRemove.getvalue(0) && shouldRemove.getvalue(1));
                        else filteredRows += text + "\n";
                    } else { // Если оператор ИЛИ
                        if (!(shouldRemove.getvalue(0)) && !(shouldRemove.getvalue(1))) filteredRows += text + "\n";
                    }
                }
                foutput(filepath, filteredRows);
                copy--;
            }

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }


    void isValidInsert(string& command) { // ф-ия проверки ввода команды insert
        string table;
        int position = command.find_first_of(' ');
        if (position != -1) { // проверка синтаксиса
            table = command.substr(0, position);
            command.erase(0, position + 1);
            if (nametables.getindex(table) != -1) { // проверка таблицы
                if (command.substr(0, 7) == "values ") { // проверка values
                    command.erase(0, 7);
                    position = command.find_first_of(' ');
                    if (position == -1) { // проверка синтаксиса ///////
                        if (command[0] == '(' && command[command.size()-1] == ')') { // проверка синтаксиса скобок и их удаление
                            command.erase(0, 1);
                            command.pop_back();
                            position = command.find(' ');
                            while (position != -1) { // удаление пробелов
                                command.erase(position);
                                position = command.find(' ');
                            }
                            insert(table, command);
                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
            } else cout << "Ошибка, нет такой таблицы!" << endl;
        } else cout << "Ошибка, нарушен синатксис команды" << endl;
    }

    void insert(string& table, string& values) { // ф-ия вставки в таблицу
        string filepath;
        int index = nametables.getindex(table); // получаем индекс таблицы(aka key)
        if (checkLockTable(table)) {
            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "close");

            // вставка значений в csv, не забывая про увеличение ключа
            filepath = "../" + nameBD + "/" + table + "/1.csv";
            int countline = CountLine(filepath);
            int fileid = 1; // номер файла csv
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
            fstream file;
            file.open(filepath, ios::app);
            int val = countlines.getvalue(index);
            file << to_string(val) + ',' + values + '\n';
            val++;
            countlines.replace(index, val);
            file.close();

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }


    bool checkLockTable(string table) {
        string filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        string check = finput(filepath);
        if (check == "open") return true;
        else return false;
    }
};


int main() {

    DataBase carshop;

    carshop.parse();
    carshop.mkdir();

    string command;
    while (true) {
        cout << endl << "Введите команду: ";
        getline(cin, command);
        carshop.checkcommand(command);
    }

    return 0;
}