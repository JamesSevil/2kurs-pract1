#pragma once

#include "includes.h"

template<typename T>
struct Array {
    T* data;       // Указатель на массив
    int capacity; // Вместимость массива
    int size;     // Текущий размер массива

    Array(); // Конструктор
    ~Array(); // Деструктор

    void resize(); // Увеличение вместимости
    void add(T value); // Добавление элемента в конец массива
    void addAt(int index, T value); // Добавление элемента по индексу
    T get(int index) const; // Получение элемента по индексу
    void remove(int index); // Удаление элемента по индексу
    void replace(int index, T value); // Замена элемента по индексу
    int length() const; // Вывод размера массива
    void print() const; // Вывод массива
};

#include "../src/array.cpp" // Включаем реализацию шаблона