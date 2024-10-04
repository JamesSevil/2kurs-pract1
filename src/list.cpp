#include "../include/list.h"

template<typename T>
Node<T>::Node(T value) : data(value), next(nullptr) {}

template<typename T>
SinglyLinkedList<T>::SinglyLinkedList() : head(nullptr) {}

template<typename T>
void SinglyLinkedList<T>::print() {
    Node<T>* current = head;
    while (current) {
        cout << current->data << " ";
        current = current->next;
    }
    cout << endl;
}

template<typename T>
void SinglyLinkedList<T>::push_front(T value) {
    Node<T>* newNode = new Node<T>(value);
    newNode->next = head;
    head = newNode;
    size++;
}

template<typename T>
void SinglyLinkedList<T>::push_back(T value) {
    Node<T>* newNode = new Node<T>(value);
    if (head == nullptr) {
        head = newNode;
    } else {
        Node<T>* current = head;
        while (current->next) {
            current = current->next;
        }
        current->next = newNode;
    }
    size++;
}

template<typename T>
void SinglyLinkedList<T>::pop_front() {
    if (head == nullptr) return;
    Node<T>* temp = head;
    head = head->next;
    delete temp;
    size--;
}

template<typename T>
void SinglyLinkedList<T>::pop_back() {
    if (head == nullptr) return;
    if (!head->next) { // если только 1 элемент
        delete head;
        head = nullptr;
        return;
    }
    Node<T>* current = head;
    while (current->next && current->next->next) {
        current = current->next;
    }
    delete current->next; // Удаляем последний элемент
    current->next = nullptr; // Обнуляем указатель
    size--;
}

template<typename T>
void SinglyLinkedList<T>::remove(T value) {
    if (head == nullptr) return;
    if (head->data == value) {
        pop_front();
        return;
    }
    Node<T>* current = head;
    while (current->next) {
        if (current->next->data == value) {
            Node<T>* temp = current->next;
            current->next = current->next->next;
            delete temp;
            return;
        }
        current = current->next;
    }
    size--;
}

template<typename T>
void SinglyLinkedList<T>::replace(int index, T newValue) {
        if (index < 0 || index >= size) {
            cout << "Index out of bounds." << endl;
            return;
        }

        Node<T>* current = head;
        for (int i = 0; i < index; i++) {
            current = current->next;
        }
        current->data = newValue;
    }

template<typename T>
int SinglyLinkedList<T>::getindex(T value) {
    Node<T>* current = head;
    int index = 0;
    while (current) {
        if (current->data == value) {
            return index; // Элемент найден, возвращаем индекс
        }
        current = current->next;
        index++;
    }
    return -1; // Если элемент не найден, возвращаем -1
}

template<typename T>
T SinglyLinkedList<T>::getvalue(int index) {
    if (index < 0 || index >= size) {
        throw out_of_range("Index out of range");
    }

    Node<T>* current = head;
    for (int i = 0; i < index; i++) {
        current = current->next;
    }

    return current->data;
}

template<typename T>
SinglyLinkedList<T>::~SinglyLinkedList() {
    while (head) {
        pop_front();
    }
}



template<typename T>
Node2<T>::Node2(T value) : data(value), next(nullptr), prev(nullptr) {}

template<typename T>
DoublyLinkedList<T>::DoublyLinkedList() : head(nullptr), tail(nullptr) {}

template<typename T>
void DoublyLinkedList<T>::print() { 
    Node2<T>* current = head;
    while (current) {
        cout << current->data << " ";
        current = current->next;
    }
    cout << endl;
}

template<typename T>
void DoublyLinkedList<T>::push_front(T value) {
    Node2<T>* newNode = new Node<T>(value);
    if (head == nullptr) {
        head = tail = newNode;
    } else {
        newNode->next = head;
        head->prev = newNode;
        head = newNode;
    }
    size++;
}

template<typename T>
void DoublyLinkedList<T>::push_back(T value) {
    Node2<T>* newNode = new Node<T>(value);
    if (tail == nullptr) {
        head = tail = newNode;
    } else {
        newNode->prev = tail;
        tail->next = newNode;
        tail = newNode;
    }
    size++;
}

template<typename T>
void DoublyLinkedList<T>::pop_front() {
    if (head == nullptr) return;
    Node2<T>* temp = head;
    head = head->next;
    if (head != nullptr) {
        head->prev = nullptr;
    } else {
        tail = nullptr; // Список стал пустым
    }
    delete temp;
    size--;
}

template<typename T>
void DoublyLinkedList<T>::pop_back() { 
    if (tail == nullptr) return; // Список пуст
    Node2<T>* temp = tail;
    tail = tail->prev;
    if (tail != nullptr) {
        tail->next = nullptr;
    } else {
        head = nullptr; // Список стал пустым
    }
    delete temp;
    size--;
}

template<typename T>
void DoublyLinkedList<T>::remove(T value) {
    Node2<T>* current = head;
    while (current) {
        if (current->data == value) {
            if (current->prev) {
                current->prev->next = current->next;
            } else {
                head = current->next; // Удаляем голову
            }
            if (current->next) {
                current->next->prev = current->prev;
            } else {
                tail = current->prev; // Удаляем хвост
            }
            delete current;
            return; // Выход после удаления первого найденного элемента
        }
        current = current->next;
    }
    size--;
}

template<typename T>
bool DoublyLinkedList<T>::find(T value) {
    Node2<T>* current = head;
    while (current) {
        if (current->data == value) {
            return true; // Элемент найден
        }
        current = current->next;
    }
    return false; // Элемент не найден
}

template<typename T>
DoublyLinkedList<T>::~DoublyLinkedList() {
    while (head) {
        pop_front();
    }
}