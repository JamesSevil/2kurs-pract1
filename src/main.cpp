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
            command = "mkdir ../" + nameBD + "/" + nametables.get(i);
            system(command.c_str());
            string filepath = "../" + nameBD + "/" + nametables.get(i) + "/1.csv";
            ofstream file;
            file.open(filepath);
            file << stlb.get(i) << endl;
            file.close();

            // Блокировка таблицы
            filepath = "../" + nameBD + "/" + nametables.get(i) + "/" + nametables.get(i) + "_lock.txt";
            file.open(filepath);
            file << "open";
            file.close();
        }
    }
};

int main() {

DataBase carshop;

carshop.parse();
carshop.mkdir();

}