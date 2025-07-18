set(TREE_TYPES "btree" "tailbtree" "lilbtree" "quit" "concurrent-simple" "concurrent-tail" "concurrent-quit" "concurrent-quit-appends" "concurrent-quit-atomic")

foreach(TREE_TYPE IN LISTS TREE_TYPES)
    if(TREE_TYPE STREQUAL "btree")
        set(TARGET_NAME "simple")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC FOR_SIMPLEBTREE=1)
    elseif(TREE_TYPE STREQUAL "tailbtree")
        set(TARGET_NAME "tail")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC FOR_TAILBTREE=1)
    elseif(TREE_TYPE STREQUAL "lilbtree")
        set(TARGET_NAME "lil")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC FOR_LILBTREE=1)
    elseif(TREE_TYPE STREQUAL "quit")
        set(TARGET_NAME "quit")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC FOR_QUIT=1)
    elseif(TREE_TYPE STREQUAL "concurrent-simple")
        set(TARGET_NAME "concurrent_simple")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC FOR_CONCURRENT_SIMPLE=1)
    elseif(TREE_TYPE STREQUAL "concurrent-tail")
        set(TARGET_NAME "concurrent_tail")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC FOR_CONCURRENT_TAIL=1)
    elseif(TREE_TYPE STREQUAL "concurrent-quit")
        set(TARGET_NAME "concurrent_quit")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC FOR_CONCURRENT_QUIT=1)
    elseif(TREE_TYPE STREQUAL "concurrent-quit-appends")
        set(TARGET_NAME "concurrent_quit_appends")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC LEAF_APPENDS FOR_CONCURRENT_QUIT_APPENDS=1)
    elseif(TREE_TYPE STREQUAL "concurrent-quit-atomic")
        set(TARGET_NAME "concurrent_quit_atomic")
        add_executable(${TARGET_NAME} tree_analysis.cpp config.cpp)
        target_compile_definitions(${TARGET_NAME} PUBLIC LEAF_APPENDS FOR_CONCURRENT_QUIT_ATOMIC=1)
    else()
        message(FATAL_ERROR "Unknown TREE_TYPE: ${TREE_TYPE}")
    endif()

    target_include_directories(${TARGET_NAME} PUBLIC include)
    target_link_libraries(${TARGET_NAME} PUBLIC spdlog::spdlog atomic)
endforeach()