class ExecutionPlanNode:
    def __init__(self, step, depth):
        self.step = step
        self.depth = depth
        self.children = []

    def add_child(self, child_node):
        self.children.append(child_node)

    def print_tree(self, level=0, is_last=True, is_last_list=None):
        if is_last_list is None:
            is_last_list = []

        prefix = ""
        for ancestor_last in is_last_list:
            prefix += "    " if ancestor_last else "│   "

        connector = "└── " if is_last else "├── "

        print(f"{prefix}{connector}{self.step}")

        is_last_list.append(is_last)

        for i, child in enumerate(self.children):
            child.print_tree(level + 1, is_last=(i == len(self.children) - 1), is_last_list=is_last_list)

        is_last_list.pop()
