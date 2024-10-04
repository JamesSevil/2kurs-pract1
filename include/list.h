#pragma once

#include "includes.h"

template<typename T>
struct Node {
    T data;
    Node* next;

    Node(T value); // конструктор узла
};
template<typename T>
struct SinglyLinkedList {
    Node<T>* head;
    int size = 0;

    SinglyLinkedList(); // конструктор листа
    ~SinglyLinkedList(); // деструктор

    void print(); // вывод списка
    void push_front(T value); // добавление элемента в начало
    void push_back(T value); // добавление элемента в конец
    void pop_front(); // удаление элемента с начала
    void pop_back(); // удаление элемента с конца
    void remove(T value); // удаление элемента по значению
    void replace(int index, T newValue); // ф-ия замены элемента по индексу
    int getindex(T value); // поиск элемента по значению
    T getvalue(int index); // поиск элемента по индексу
};

template <typename T>
struct Node2 {
    T data;
    Node2* next;
    Node2* prev;

    Node2(T value); // конструктор узла
};
template <typename T>
struct DoublyLinkedList {
    Node2<T>* head;
    Node2<T>* tail;
    int size = 0;

    DoublyLinkedList(); // конструктор листа
    ~DoublyLinkedList(); // деструктор

    void print(); // вывод списка
    void push_front(T value); // добавление элемента в начало
    void push_back(T value); // добавление элемента в конец
    void pop_front(); // удаление элемента в начале
    void pop_back(); // удаление элемента в конце
    void remove(T value); // удаление элемента по значению
    bool find(T value); // поиска элемента по значению
};

#include "../src/list.cpp" // Включаем реализацию шаблона