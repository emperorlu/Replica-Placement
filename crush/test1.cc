#include <cstdint>
#include <iostream>

#define ALIAS(fn)   __attribute__ ((alias (#fn), used))

static char buf[256];

extern "C"
{
    void* my_malloc(size_t size) {
        std::cout << "custom malloc\n";
        return buf;
    }

    void* my_aligned_alloc(size_t align, size_t size) {
        std::cout << "custom aligned alloc\n";
        return buf;
    }

    void my_free(void* p) {
        std::cout << "custom free\n";
    }

    void* malloc(size_t size) ALIAS(my_malloc);
    void* aligned_alloc(size_t align, size_t size) ALIAS(my_aligned_alloc);
    void free(void* ptr) ALIAS(my_free);
}

struct S {
    S() {}
    uint32_t a;
}__attribute__((aligned(64)));

struct S1 {
    uint32_t a;
};

int main() {
    auto s = new S();
    delete s;
}
