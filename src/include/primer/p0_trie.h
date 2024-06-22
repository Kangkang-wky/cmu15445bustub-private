//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 * TrieNode 是 Trie tree 的基类
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   * 构建 Trie 树的基类: 成员变量 key_char
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) {}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   * 移动构造函数: 注意 children 成员变量中 map<char, unique_ptr> 需要移动构造函数
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    key_char_ = other_trie_node.key_char_;
    is_end_ = other_trie_node.is_end_;
    children_ = std::move(other_trie_node.children_);
  }

  /**
   * @brief Destroy the TrieNode object.
   * 基类 ~TrieNode() 虚函数, 析构函数可以是虚函数
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   * 以下函数接口全部采用尾置返回类型, 本意是在模板类型推导中使用,但为了通过检查使用
   * 判断该 node 有没有 对应 key_char 的孩子节点
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  auto HasChild(char key_char) const -> bool { return children_.count(key_char) > 0; }

  /**
   * TODO(P0): Add implementation
   * 判断是否没有孩子节点
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  auto HasChildren() const -> bool { return !children_.empty(); }
  /**
   * TODO(P0): Add implementation
   * 判断是否是叶子节点
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  auto IsEndNode() const -> bool { return is_end_; }
  /**
   * TODO(P0): Add implementation
   * 给出该节点的 char
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  auto GetKeyChar() const -> char { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * 对当前节点插入一个孩子节点, 如果有孩子就不插入
   *
   * 注意此处要求的是返回 unique_ptr 的指针, 因为并不需要 unique_ptr 的所有权, 只需要能够访问 unique_ptr 的数据
   * 以及注意此处传入的应该是右值
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  auto InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) -> std::unique_ptr<TrieNode> * {
    if (HasChild(key_char) || key_char != child->GetKeyChar()) {
      return nullptr;
    }
    children_[key_char] = std::move(child);
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  auto GetChildNode(char key_char) -> std::unique_ptr<TrieNode> * {
    if (children_.count(key_char) == 0) {
      return nullptr;
    }
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * 移除某一个孩子节点
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (children_.count(key_char) == 0) {
      return;
    }
    children_.erase(key_char);
  }

  /**
   * TODO(P0): Add implementation
   *
   * 将 Trie tree 节点设置为 应该 end 的节点
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
  // key_char_ 是字典树 node 上的 key
  // is_end_ 是判断字典树 node 最后的叶子节点
  // children_ 是记录node 的孩子节点 hashmap 记录 char-TireNode* 的这样的 hashmap
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 * TrieNodeWithValue 继承自 TrieNode 类, 主要保存那些 is_end_ 恒为 true 代表真实值
 * 的节点, 主要保存那些终止节点, with_value 表示那些实际保存 value 值的节点
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   * 此处构造函数通过使用基类的移动构造初始化, 使用 std::forward 转发右值
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::forward<TrieNode>(trieNode)), value_(value) {
    SetEndNode(true);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   * 基类构造函数初始化
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value) { SetEndNode(true); }

  /**
   * @brief Destroy the Trie Node With Value object
   * 使用默认析构函数, 此处并没有堆上动态分配的资源需要处理
   * override 表示这是一个虚函数需要覆盖父类的虚函数实现
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  auto GetValue() const -> T { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 * Trie tree class 字典树类, 包含两个成员变量, 一个是根节点, 一个是 trie 树的读写锁
 * 读写锁其实只是对 std::shared_mutex 做了一个简单的封装, 封装其调用
 * 读写锁设计较为简单, 在 function_body 处锁住, 在退出函数的时候解锁
 * 以下主要实现 Trie tree 的三个接口:
 * (1) 插入:  用 key 遍历 Trie tree
 *  1. 对应 key 的 Trie node 不存在    2. 对应 key 的 Trie node 存在但不是 withvalue 的
 *  3. 对应 key 的 Trie node 存在 且是 withvalue 的
 * (2) 删除：
 * (3) getvalue: 取值
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() { root_ = std::make_unique<TrieNode>('\0'); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  auto Insert(const std::string &key, T value) -> bool {
    const size_t key_length = key.size();
    // 空字符串直接返回
    if (key_length == 0) {
      return false;
    }
    latch_.WLock();
    // 从 root 节点出发
    auto node = &(this->root_);
    // parent 节点用来更新
    std::unique_ptr<TrieNode> *parent;
    // 遍历所有 key 中的字符, 不存在
    for (size_t i = 0; i < key_length; i++) {
      char ch = key[i];
      // 此处防御性编程一下
      if (node->get()->GetChildNode(ch) == nullptr) {
        node->get()->InsertChildNode(ch, std::make_unique<TrieNode>(key[i]));
      }
      parent = node;
      node = node->get()->GetChildNode(ch);
    }
    // key 已经存在, 说明 withvalue 返回 false 即可
    if (node->get()->IsEndNode()) {
      latch_.WUnlock();
      return false;
    }
    // true 将原来得 without value 替换为 with value 的节点
    // 获取子节点的裸体指针, 创建一个 withvalue 节点
    auto without_node_ptr = node->get();
    auto withvalue_node = std::make_unique<TrieNodeWithValue<T>>(std::move(*without_node_ptr), value);

    // 移除旧的 withoutvalue 节点, 插入新的 withvalue 节点
    parent->get()->RemoveChildNode(key[key_length - 1]);
    parent->get()->InsertChildNode(key[key_length - 1], std::move(withvalue_node));

    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   *
   * key 为空 或者 未找到直接 false 即可
   * 其他情况需要 找到终止节点, withvalue node
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  auto Remove(const std::string &key) -> bool {
    const size_t key_length = key.size();
    // key 为空
    if (key_length == 0) {
      return false;
    }
    latch_.WLock();
    auto node = &(this->root_);

    // 存搜索路径
    std::stack<std::tuple<char, std::unique_ptr<TrieNode> *>> st_path;

    // 搜索: key 不在 Tire tree 上
    for (size_t i = 0; i < key_length; i++) {
      if (node->get()->GetChildNode(key[i]) == nullptr) {
        latch_.WUnlock();
        return false;
      }
      // 这里使用 push 似乎无法通过 clang-tidy 检查
      st_path.emplace(key[i], node);
      node = node->get()->GetChildNode(key[i]);
    }

    while (!st_path.empty()) {
      auto top = st_path.top();
      st_path.pop();
      auto [ch, parent_node] = top;

      auto child_node = parent_node->get()->GetChildNode(ch);

      if (child_node != nullptr && child_node->get()->HasChildren()) {
        continue;
      }
      parent_node->get()->RemoveChildNode(ch);
    }
    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * getvalue 做三项检查来判断是否为 false, 并返回泛型空值
   * (1) key 空
   * (2) key 不在 Trie tree 上
   * (3) value 的类型 不匹配
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  auto GetValue(const std::string &key, bool *success) -> T {
    const size_t key_length = key.size();
    // key 为空
    if (key_length == 0) {
      *success = false;
      return T();
    }
    latch_.RLock();
    auto node = &(this->root_);

    // 搜索: key 不在 Tire tree 上
    for (size_t i = 0; i < key_length; i++) {
      if (node->get()->GetChildNode(key[i]) == nullptr) {
        *success = false;
        latch_.RUnlock();
        return T();
      }
      node = node->get()->GetChildNode(key[i]);
    }

    // key 不在终止节点上
    if (!node->get()->IsEndNode()) {
      *success = false;
      latch_.RUnlock();
      return T();
    }

    // true return withvalue_node
    // 根据 assignment 中的提示将 Tire tree 中的 node 节点的 类型与 模板类型比较, 如果
    // 类型的话 置 false
    auto withvalue_node = dynamic_cast<TrieNodeWithValue<T> *>(node->get());
    if (withvalue_node != nullptr) {
      *success = true;
      latch_.RUnlock();
      return withvalue_node->GetValue();
    }
    // else & others
    *success = false;
    latch_.RUnlock();
    return T();
  }
};
}  // namespace bustub
