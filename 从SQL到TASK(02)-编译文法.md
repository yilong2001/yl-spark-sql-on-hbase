
## LL(1) 、LR(0)、SLR、LR(1) 文法

### LL(1)
LL(1)定义：一个文法G是LL（1）的，当且仅当对于G的每一个非终结符A的任何两个不同产生式 A→α|β，下面的条件成立：SELECT( A→α)∩SELECT( A→β)=dd,其中，α|β不能同时 ε.

LL(1)的意思 ：第一个L,指的是从左往右处理输入，第二个L,指的是它为输入生成一个最左推导。
1指的是向前展望1个符号。

L(1)文法是上下文无关文法的一个子集。它用的方法是自顶向下的(递归式的处理)。它要求生成的预测分析表的每一个项目至多只能有一个生成式。

上面的定义说的是，任何两个不同的产生式 A→α和 A→β,选择A→α或者 A→β是不能有冲突的，即SELECT( A→α)∩SELECT( A→β)=，具体来说，就是，

第一:First( A→α) ∩First( A→β)=Null,首符集不能有交集，否则当交集中的元素出现时，选择哪个产生式进行推导是不确定的，（这其中也包含了α|β不能同时ε，否则交集就是{ε}不为空）；

第二：若任何一个产生式β，有ε属于First(β),应有First(A) ∩ Follow(A)为空（当ε属于First(β)，则 A 有可能被空串代替，那么就要看 A 的下一个字符，即Follow集，即要求 Follow 集和 First 集不能相交，否则可能发生冲突）。

### LR
LR文法：定义：如果某一文法能够构造一张LR分析表，使得表中每一个元素至多只有一种明确动作，则该文法称为LR文法。

LR文法是一种自底向上的文法。

LR(0)文法判定：如果文法对应的自动机中 不存在 移进-归约冲突 和 归约-归约 冲突 则为 LR(0)文法。
换句话说LR(0)文法分析不能解决这两种冲突，所以范围最小。

移进-归约冲突：就是在同一个项集族中同时出现了可以移进的产生式和可以归约的产生式。

归约-归约冲突类似。

### SLR(1)
SLR(1)定义：满足下面两个条件的文法是 SLR(1) 文法

a.对于在s中的任何项目 A→α.Xβ, 当 X 是一个终结符，且 X 在 Follow(B) 中时，s 中没有完整的项目 B→r.

b.对于在s中的任何两个完整项目 A→α.和 B→β.,Follow(A) ∩ Follow(B) 为空。

### 比较
LL(1)就是向前只搜索1个符号，即与FIRST()匹配，如果FIRST为空则还要考虑Follow。

LR需要构造一张LR分析表，此表用于当面临输入字符时，将它移进，规约（即自下而上分析思想），接受还是出错。

LR(0)找出句柄前缀，构造分析表，然后根据输入符号进行规约。不考虑先行，只要出现终结符就移进，只要出现归约状态，就无条件归约,这样子可能出现归约－移进，归约－归约冲突。

SLR(1)使用LR(0)时若有归约－归约冲突，归约－移进冲突，需要看先行，把有问题的地方向前搜索一次。

---------------------

本文来自 acmdream 的CSDN 博客 ，全文地址请点击：https://blog.csdn.net/acmdream/article/details/53375104?utm_source=copy 

## ANTLR 

### 词法分析器（Lexer）
词法分析器又称为Scanner，Lexical analyser和Tokenizer。
程序设计语言通常由keyword和严格定义的语法结构组成。
词法分析器的工作是分析量化那些本来毫无意义的字符流，将他们翻译成离散的字符组（也就是一个一个的Token）括keyword，标识符，符号（symbols）和操作符供语法分析器使用。

### 语法分析器（Parser）
编译器又称为Syntactical analyser。在分析字符流的时候，Lexer不关心所生成的单个Token的语法意义及其与上下文之间的关系，而这就是Parser的工作。
语法分析器将收到的Tokens组织起来，并转换成为目标语言语法定义所同意的序列。
不管是Lexer还是Parser都是一种识别器，Lexer是字符序列识别器而Parser是Token序列识别器。

### ANTLR 
ANTLR 包含词法分析、语法分析两部分。ANTLR 根据 m4 语法文件 生成对应的词法/语法分析器。
通过自动生成的词法、语法分析器，就可以将 输入内容 转换成其它形式（如AST—Abstract Syntax Tree，抽象语法树）。

## 语法树 (AST)
编译技术中用语法树来更直观的表示一个句型的推导过程。

### 上下文无关文法语法树
定义：给定上下文无关文法G[S]，它的语法树的每一个节点都有一个G[S]文法的符号与之对应。
S为语法树的根节点。如果一个节点有子节点。则这个节点对应的符号一定是非终结符。
如果一个节点对应的符号为A，它的子节点对应的符号分别为A1，A2，A3…..Ak，那么G[S]文法中一定有一个规则为：A=>A1 A2 A3 …..Ak。
满足这些规定的树语法树也叫推导树。

### 分析方法
有了语法树就可以清晰的看到句型结构，可以很容易从语法树中获得必要信息，这个获取信息的过程就是语法分析。

### 自顶向下的分析方法和自下而上的分析方法
语法分析方法分两大类：自顶向下的分析方法和自下而上的分析方法。
ANTLR使用的是自顶向下的分析方法。
自顶向下的分析方法的思路是从起始规则开始选择适当的规则反复推导，直到推导出待分析的语型。
如果推导失败则反回选择其它规则进行推导（这个过程叫做回朔（backtrack）），如果所有规则都失败说明这个句型是非法的。
例如：
D1[S]文法：
S => aBd
B => b
B = > bc

对于abcd句型进行自顶向下分析，
(1)选择规则S => aBd
(2)对非终结符B的推导，先选择B => b 推导出 S => abd 这和句型 abcd 不同所以推导失败。
(3)现在返回到对B的推导，选择另一个规则 B => bc 得到 S => abcd，这次推导成功。
