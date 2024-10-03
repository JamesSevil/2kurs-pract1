#include "../include/array.h"

template<typename T>
Array<T>::Array() : capacity(10), size(0) {
    data = new T[capacity];
}

template<typename T>
Array<T>::~Array() {
    delete[] data;
}

template<typename T>
void Array<T>::resize() {
    capacity *= 2;
    T* newData = new T[capacity];
    for (int i = 0; i < size; ++i) {
        newData[i] = data[i]; // Копируем старые данные
    }
    delete[] data; // Освобождаем старый массив
    data = newData; // Указываем на новый массив
}

template<typename T>
void Array<T>::add(T value) {
    if (size == capacity) {
        resize(); // Увеличиваем массив, если он заполнен
    }
    data[size++] = value;
}

template<typename T>
void Array<T>::addAt(int index, T value) {
    if (index > size) {
        throw out_of_range("Index out of range");
    }
    if (size == capacity) {
        resize(); // если заполнен - увеличить
    }
    for (int i = size; i > index; --i) {
        data[i] = data[i - 1]; // Сдвигаем элементы вправо
    }
    data[index] = value;
    ++size;
}

template<typename T>
T Array<T>::get(int index) const {
    if (index >= size) {
        throw out_of_range("Index out of range");
    }
    return data[index];
}

template<typename T>
void Array<T>::remove(int index) {
    if (index >= size) {
        throw out_of_range("Index out of range");
    }
    for (int i = index; i < size - 1; ++i) {
        data[i] = data[i + 1]; // Сдвигаем элементы влево
    }
    --size; // уменьшаем размер
}

template<typename T>
void Array<T>::replace(int index, T value) {
    if (index >= size) {
        throw out_of_range("Index out of range");
    }
    data[index] = value; // Заменяем элемент по индексу
}

template<typename T>
int Array<T>::length() const {
    return size;
}

template<typename T>
void Array<T>::print() const {
    for (int i = 0; i < size; ++i) {
        cout << data[i] << " ";
    }
    cout << endl;
}