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

    struct Where { // структура для фильтрации
        string table;
        string column;
        string value;
        string logicalOP;
        bool check; // В частности для select, проверка условия(если просто условие - true, если условиестолбец - false)
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

            // ключ
            filepath = "../" + nameBD + "/" + nametables.getvalue(i) + "/" + nametables.getvalue(i) + "_pk_sequence.txt";
            file.open(filepath);
            file << "1";
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
            isValidSelect(command);
        } else if (command == "exit") {
            exit(0);
        } else cout << "Ошибка, неизвестная команда!" << endl; 
    }


    // ф-ии делита
    void isValidDel(string& command) { // ф-ия обработки команды DELETE
        string table, conditions;
        int position = command.find_first_of(' ');
        if (position != -1) {
            table = command.substr(0, position);
            conditions = command.substr(position + 1);
        } else table = command;
        if (nametables.getindex(table) != -1) { // проверка таблицы
            if (conditions.empty()) { // если нет условий, удаляем все
                del(table);
            } else {
                if (conditions.substr(0, 6) == "where ") { // проверка наличия where
                    conditions.erase(0, 6);
                    SinglyLinkedList<Where> cond;
                    Where where;
                    position = conditions.find_first_of(' '); ////
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
                                if (position == -1) { // если нет лог. оператора
                                    where.value = conditions;
                                    delWithValue(table, where.column, where.value);
                                } else { // если есть логический оператор
                                    where.value = conditions.substr(0, position);
                                    conditions.erase(0, position+1);
                                    cond.push_back(where);
                                    position = conditions.find_first_of(' ');
                                    if ((position != -1) && (conditions.substr(0, 2) == "or" || conditions.substr(0, 3) == "and")) {
                                        where.logicalOP = conditions.substr(0, position);
                                        conditions.erase(0, position + 1);
                                        position = conditions.find_first_of(' ');
                                        if (position != -1) {
                                            where.column = conditions.substr(0, position);
                                            conditions.erase(0, position+1);
                                            index = nametables.getindex(table);
                                            str = stlb.getvalue(index);
                                            stringstream iss(str);
                                            bool check = false;
                                            while (getline(iss, str, ',')) if (str == where.column) check = true;
                                            if (check) { // проверка столбца
                                                if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                                    conditions.erase(0, 2);
                                                    position = conditions.find_first_of(' ');
                                                    if (position == -1) {
                                                        where.value = conditions;
                                                        cond.push_back(where);
                                                        delWithLogic(cond, table);
                                                    } else cout << "Ошибка, нарушен синтаксис команды4!" << endl;
                                                } else cout << "Ошибка, нарушен синтаксис команды3!" << endl;
                                            } else cout << "Ошибка, нет такого столбца!" << endl;
                                        } else cout << "Ошибка, нарушен синтаксис команды2!" << endl;
                                    } else cout << "Ошибка, нарушен синтаксис команды1!" << endl;
                                }
                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                        } else cout << "Ошибка, нет такого столбца!" << endl;
                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                } else cout << "Ошибка, нарушен синтаксис команды!"<< endl;
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


    // ф-ии инсерта
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
        string filepath = "../" + nameBD + "/" + table + "/" + table + "_pk_sequence.txt";
        int index = nametables.getindex(table); // получаем индекс таблицы(aka key)
        string val = finput(filepath);
        int valint = stoi(val);
        valint++;
        foutput(filepath, to_string(valint));

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
            file << val + ',' + values + '\n';
            file.close();

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            foutput(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }


    // ф-ии селекта
    void isValidSelect(string& command) { // ф-ия проверки ввода команды select
        Where conditions;
        SinglyLinkedList<Where> cond;

        if (command.find_first_of("from") != -1) {
            // работа со столбцами
            while (command.substr(0, 4) != "from") {
                string token = command.substr(0, command.find_first_of(' '));
                if (token.find_first_of(',') != -1) token.pop_back(); // удаляем запятую
                command.erase(0, command.find_first_of(' ') + 1);
                if (token.find_first_of('.') != -1) token.replace(token.find_first_of('.'), 1, " ");
                else {
                    cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    return;
                }
                stringstream ss(token);
                ss >> conditions.table >> conditions.column;
                bool check = false;
                int i;
                for (i = 0; i < nametables.size; ++i) { // проверка, сущ. ли такая таблица
                    if (conditions.table == nametables.getvalue(i)) {
                        check = true;
                        break;
                    }
                }
                if (!check) {
                    cout << "Нет такой таблицы!" << endl;
                    return;
                }
                check = false;
                stringstream iss(stlb.getvalue(i));
                while (getline(iss, token, ',')) { // проверка, сущ. ли такой столбец
                    if (token == conditions.column) {
                        check = true;
                        break;
                    }
                }
                if (!check) {
                    cout << "Нет такого столбца" << endl;
                    return;
                }
                cond.push_back(conditions);
            }

            command.erase(0, command.find_first_of(' ') + 1); // скип from

            // работа с таблицами
            int iter = 0;
            while (!command.empty()) { // пока строка не пуста
                string token = command.substr(0, command.find_first_of(' '));
                if (token.find_first_of(',') != -1) {
                    token.pop_back();
                }
                int position = command.find_first_of(' ');
                if (position != -1) command.erase(0, position + 1);
                else command.erase(0);
                if (iter + 1 > cond.size || token != cond.getvalue(iter).table) {
                    cout << "Ошибка, указаные таблицы не совпадают или их больше!" << endl;
                    return;
                }
                if (command.substr(0, 5) == "where") break; // также заканчиваем цикл если встретился WHERE
                iter++;
            }
            if (command.empty()) {
                select(cond);
            } else {
                if (command.find_first_of(' ') != -1) {
                    command.erase(0, 6);
                    int position = command.find_first_of(' ');
                    if (position != -1) {
                        string token = command.substr(0, position);
                        command.erase(0, position + 1);
                        if (token.find_first_of('.') != -1) {
                            token.replace(token.find_first_of('.'), 1, " ");
                            stringstream ss(token);
                            string table, column;
                            ss >> table >> column;
                            if (table == cond.getvalue(0).table) { // проверка таблицы в where
                                position = command.find_first_of(' ');
                                if ((position != -1) && (command[0] == '=')) {
                                    command.erase(0, position + 1);
                                    position = command.find_first_of(' ');
                                    if (position == -1) { // если нет лог. операторов
                                        if (command.find_first_of('.') == -1) { // если просто значение
                                            conditions.value = command;
                                            conditions.check = true;
                                            selectWithValue(cond, table, column, conditions);
                                        } else { // если столбец
                                            command.replace(command.find_first_of('.'), 1, " ");
                                            stringstream iss(command);
                                            iss >> conditions.table >> conditions.column;
                                            conditions.check = false;
                                            selectWithValue(cond, table, column, conditions);
                                        }

                                    } else { // если есть лог. операторы
                                        SinglyLinkedList<Where> values;
                                        token = command.substr(0, position);
                                        command.erase(0, position + 1);
                                        if (token.find_first_of('.') == -1) { // если просто значение
                                            conditions.value = token;
                                            conditions.check = true;
                                            values.push_back(conditions);
                                        } else { // если столбец
                                            token.replace(token.find_first_of('.'), 1, " ");
                                            stringstream stream(token);
                                            stream >> conditions.table >> conditions.column;
                                            conditions.check = false;
                                            values.push_back(conditions);
                                        }
                                        position = command.find_first_of(' ');
                                        if ((position != -1) && (command.substr(0, 2) == "or" || command.substr(0, 3) == "and")) {
                                            conditions.logicalOP = command.substr(0, position);
                                            command.erase(0, position + 1);
                                            position = command.find_first_of(' ');
                                            if (position != -1) {
                                                token = command.substr(0, position);
                                                command.erase(0, position + 1);
                                                if (token.find_first_of('.') != -1) {
                                                    token.replace(token.find_first_of('.'), 1, " ");
                                                    stringstream istream(token);
                                                    SinglyLinkedList<string> tables;
                                                    SinglyLinkedList<string> columns;
                                                    tables.push_back(table);
                                                    columns.push_back(column);
                                                    istream >> table >> column;
                                                    tables.push_back(table);
                                                    columns.push_back(column);
                                                    if (table == cond.getvalue(0).table) { // проверка таблицы в where
                                                        position = command.find_first_of(' ');
                                                        if ((position != -1) && (command[0] == '=')) {
                                                            command.erase(0, position + 1);
                                                            position = command.find_first_of(' ');
                                                            if (position == -1) { // если нет лог. операторов
                                                                if (command.find_first_of('.') == -1) { // если просто значение
                                                                    conditions.value = command.substr(0, position);
                                                                    conditions.check = true;
                                                                    command.erase(0, position + 1);
                                                                    values.push_back(conditions);
                                                                    selectWithLogic(cond, tables, columns, values);
                                                                } else { // если столбец
                                                                    token = command.substr(0, position);
                                                                    token.replace(token.find_first_of('.'), 1, " ");
                                                                    command.erase(0, position + 1);
                                                                    stringstream stream(token);
                                                                    stream >> conditions.table >> conditions.column;
                                                                    conditions.check = false;
                                                                    values.push_back(conditions);
                                                                    selectWithLogic(cond, tables, columns, values);
                                                                }
                                                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                    } else cout << "Ошибка, таблица в where не совпадает с начальной!" << endl;
                                                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                    }
                                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                            } else cout << "Ошибка, таблица в where не совпадает с начальной!" << endl;
                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
            }
        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    }

    void select(SinglyLinkedList<Where>& conditions) { // ф-ия обычного селекта
        for (int i = 0; i < conditions.size; ++i) {
            bool check = checkLockTable(conditions.getvalue(i).table);
            if (!check) {
                cout << "Ошибка, таблица открыта другим пользователем!" << endl;
                return;
            }
        }
        string filepath;
        for (int i = 0; i < conditions.size; ++i) {
            filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
            foutput(filepath, "close");
        }

        SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов после "select"
        SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
        sample(stlbindex, tables); // выборка

        for (int i = 0; i < conditions.size; ++i) {
            filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
            foutput(filepath, "open");
        }
    }

    void selectWithValue(SinglyLinkedList<Where>& conditions, string& table, string& stolbec, struct Where value) { // ф-ия селекта с where для обычного условия
        for (int i = 0; i < conditions.size; ++i) {
            bool check = checkLockTable(conditions.getvalue(i).table);
            if (!check) {
                cout << "Ошибка, таблица открыта другим пользователем!" << endl;
                return;
            }
        }
        string filepath;
        for (int i = 0; i < conditions.size; ++i) {
            filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
            foutput(filepath, "close");
        }

        SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов
        int stlbindexval = findIndexStlbCond(table, stolbec); // узнаем индекс столбца условия
        int stlbindexvalnext = findIndexStlbCond(value.table, value.column); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
        SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
        SinglyLinkedList<string> column = findStlbTable(conditions, tables, stlbindexvalnext, value.table);; // записываем колонки таблицы условия после '='(нужно если условиестолбец)
        
        // фильтруем нужные строки
        for (int i = 0; i < conditions.size; ++i) {
            if (conditions.getvalue(i).table == table) { 
                stringstream stream(tables.getvalue(i));
                string str;
                string filetext;
                int iterator = 0; // нужно для условиястолбец 
                while (getline(stream, str)) {
                    stringstream istream(str);
                    string token;
                    int currentIndex = 0;
                    while (getline(istream, token, ',')) {
                        if (value.check) { // для простого условия
                            if (currentIndex == stlbindexval && token == value.value) {
                                filetext += str + '\n';
                                break;
                            }
                            currentIndex++;
                        } else { // для условиястолбец
                            if (currentIndex == stlbindexval && token == column.getvalue(iterator)) {
                            filetext += str + '\n';
                            }
                            currentIndex++;
                        }
                    }
                    iterator++;
                }
                tables.replace(i, filetext);
            }
        }

        sample(stlbindex, tables); // выборка

        for (int i = 0; i < conditions.size; ++i) {
            filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
            foutput(filepath, "open");
        }
    }

    void selectWithLogic(SinglyLinkedList<Where>& conditions, SinglyLinkedList<string>& table, SinglyLinkedList<string>& stolbec, SinglyLinkedList<Where>& value) {
        for (int i = 0; i < conditions.size; ++i) {
            bool check = checkLockTable(conditions.getvalue(i).table);
            if (!check) {
                cout << "Ошибка, таблица открыта другим пользователем!" << endl;
                return;
            }
        }
        string filepath;
        for (int i = 0; i < conditions.size; ++i) {
            filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
            foutput(filepath, "close");
        }

        SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов после "select"
        SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
        SinglyLinkedList<int> stlbindexval;// узнаем индексы столбца условия
        for (int i = 0; i < stolbec.size; ++i) {
            int index = findIndexStlbCond(table.getvalue(i), stolbec.getvalue(i));
            stlbindexval.push_back(index);
        }
        SinglyLinkedList<int> stlbindexvalnext; // узнаем индекс столбца условия после '='(нужно если условиестолбец)
        for (int i = 0; i < value.size; ++i) {
            int index = findIndexStlbCond(value.getvalue(i).table, value.getvalue(i).column); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
            stlbindexvalnext.push_back(index);
        }
        SinglyLinkedList<string> column;
        for (int j = 0; j < value.size; ++j) {
            if (!value.getvalue(j).check) { // если условие столбец
                column = findStlbTable(conditions, tables, stlbindexvalnext.getvalue(j), value.getvalue(j).table);
            }
        }

        // фильтруем нужные строки
        for (int i = 0; i < conditions.size; ++i) {
            if (conditions.getvalue(i).table == table.getvalue(0)) {
                stringstream stream(tables.getvalue(i));
                string str;
                string filetext;
                int iterator = 0; // нужно для условиястолбец 
                while (getline(stream, str)) {
                    SinglyLinkedList<bool> checkstr;
                    for (int j = 0; j < value.size; ++j) {
                        stringstream istream(str);
                        string token;
                        int currentIndex = 0;
                        bool check = false;
                        while (getline(istream, token, ',')) {
                            if (value.getvalue(j).check) { // если просто условие
                                if (currentIndex == stlbindexval.getvalue(j) && token == value.getvalue(j).value) {
                                    check = true;
                                    break;
                                }
                                currentIndex++;
                            } else { // если условие столбец
                                if (currentIndex == stlbindexval.getvalue(j) && token == column.getvalue(iterator)) {
                                    check = true;
                                    break;
                                }
                                currentIndex++;
                            }
                        }
                        checkstr.push_back(check);
                    }
                    if (value.getvalue(1).logicalOP == "and") { // Если оператор И
                        if (checkstr.getvalue(0) && checkstr.getvalue(1)) filetext += str + "\n";
                    } else { // Если оператор ИЛИ
                        if (!checkstr.getvalue(0) && !checkstr.getvalue(1));
                        else filetext += str + "\n";
                    }
                    iterator++;
                }
                tables.replace(i, filetext);
            }
        }

        sample(stlbindex, tables); // выборка

        for (int i = 0; i < conditions.size; ++i) {
            filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
            foutput(filepath, "open");
        }
    }

 
    // Вспомогательные ф-ии, чтобы избежать повтора кода в основных ф-иях
    bool checkLockTable(string table) { // ф-ия проверки, закрыта ли таблица
        string filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        string check = finput(filepath);
        if (check == "open") return true;
        else return false;
    }

    SinglyLinkedList<int> findIndexStlb(SinglyLinkedList<Where>& conditions) { // ф-ия нахождения индекса столбцов(для select)
        SinglyLinkedList<int> stlbindex;
        for (int i = 0; i < conditions.size; ++i) {
            int index = nametables.getindex(conditions.getvalue(i).table);
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
        return stlbindex;
    }

    int findIndexStlbCond(string table, string stolbec) { // ф-ия нахождения индекса столбца условия(для select)
        int index = nametables.getindex(table);
        string str = stlb.getvalue(index);
        stringstream ss(str);
        int stlbindex = 0;
        while (getline(ss, str, ',')) {
            if (str == stolbec) break;
            stlbindex++;
        }
        return stlbindex;
    }

    SinglyLinkedList<string> textInFile(SinglyLinkedList<Where>& conditions) { // ф-ия инпута текста из таблиц(для select)
        string filepath;
        SinglyLinkedList<string> tables;
        for (int i = 0; i < conditions.size; ++i) {
            string filetext;
            int index = nametables.getindex(conditions.getvalue(i).table);
            int iter = 0;
            do {
                iter++;
                filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + to_string(iter) + ".csv";
                string text = finput(filepath);
                int position = text.find('\n'); // удаляем названия столбцов
                text.erase(0, position + 1);
                filetext += text + '\n';
            } while (iter != fileindex.getvalue(index));
            tables.push_back(filetext);
        }
        return tables;
    }

    SinglyLinkedList<string> findStlbTable(SinglyLinkedList<Where>& conditions, SinglyLinkedList<string>& tables, int stlbindexvalnext, string table) { // ф-ия инпута нужных колонок из таблиц для условиястолбец(для select)
        SinglyLinkedList<string> column;
        for (int i = 0; i < conditions.size; ++i) {
            if (conditions.getvalue(i).table == table) {
                stringstream stream(tables.getvalue(i));
                string str;
                while (getline(stream, str)) {
                    stringstream istream(str);
                    string token;
                    int currentIndex = 0;
                    while (getline(istream, token, ',')) {
                        if (currentIndex == stlbindexvalnext) {
                            column.push_back(token);
                            break;
                        }
                        currentIndex++;
                    }
                }
            }
        }
        return column;
    }

    void sample(SinglyLinkedList<int>& stlbindex, SinglyLinkedList<string>& tables) { // ф-ия выборки(для select)
       for (int i = 0; i < tables.size - 1; ++i) {
            stringstream onefile(tables.getvalue(i));
            string token;
            while (getline(onefile, token)) {
                string needstlb;
                stringstream ionefile(token);
                int currentIndex = 0;
                while (getline(ionefile, token, ',')) {
                    if (currentIndex == stlbindex.getvalue(i)) {
                        needstlb = token;
                        break;
                    }
                    currentIndex++;
                }
                stringstream twofile(tables.getvalue(i + 1));
                while (getline(twofile, token)) {
                    stringstream itwofile(token);
                    currentIndex = 0;
                    while (getline(itwofile, token, ',')) {
                        if (currentIndex == stlbindex.getvalue(i + 1)) {
                            cout << needstlb << ' ' << token << endl;
                            break;
                        }
                        currentIndex++;
                    }
                }
            } 
        } 
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