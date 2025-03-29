#pragma once

namespace sort_utils {

template <typename key_type, typename value_type>
void heapify(key_type *keys, value_type *values, int n, int i, int left) {
    int largest = i;
    int l = 2 * i + 1 - left;
    int r = 2 * i + 2 - left;

    if (l < n && keys[left + l] > keys[left + largest]) {
        largest = l;
    }

    if (r < n && keys[left + r] > keys[left + largest]) {
        largest = r;
    }

    if (largest != i) {
        std::swap(keys[left + i], keys[left + largest]);
        std::swap(values[left + i], values[left + largest]);
        heapify(keys, values, n, largest, left);
    }
}

template <typename key_type, typename value_type>
void heapsort(key_type *keys, value_type *values, int left, int right) {
    int n = right - left + 1;
    for (int i = n / 2 - 1; i >= 0; --i) {
        heapify(keys, values, n, i, left);
    }

    for (int i = n - 1; i > 0; --i) {
        std::swap(keys[left], keys[left + i]);
        std::swap(values[left], values[left + i]);
        heapify(keys, values, i, 0, left);
    }
}

template <typename key_type, typename value_type>
int partition(key_type *keys, value_type *values, int left, int right) {
    int mid = left + (right - left) / 2;

    // Median-of-three pivot selection - helps with nearly sorted data.
    if (keys[mid] < keys[left]) std::swap(keys[left], keys[mid]);
    if (keys[right] < keys[left]) std::swap(keys[left], keys[right]);
    if (keys[right] < keys[mid]) std::swap(keys[mid], keys[right]);

    key_type pivot = keys[mid];
    std::swap(keys[mid], keys[right]);  // Move pivot to the end

    int i = left - 1;
    for (int j = left; j < right; ++j) {
        if (keys[j] <= pivot) {
            ++i;
            std::swap(keys[i], keys[j]);
            std::swap(values[i], values[j]);
        }
    }
    std::swap(keys[i + 1], keys[right]);
    std::swap(values[i + 1], values[right]);
    return i + 1;
}

template <typename key_type, typename value_type>
void introsort(key_type *keys, value_type *values, int left, int right,
               int depth_limit) {
    if (left >= right) return;

    if (depth_limit == 0) {
        heapsort(keys, values, left, right);
        return;
    }

    int partition_index = partition(keys, values, left, right);

    introsort(keys, values, left, partition_index - 1, depth_limit - 1);
    introsort(keys, values, partition_index + 1, right, depth_limit - 1);
}

}  // namespace sort_utils