# general-flask-store-and-qrcode 

# 通用Flask后端API及管理页面 

---

## 版权声明 

## Copyright (c) 2026 ai摸鱼的程序员

## 本项目基于 Apache 2.0 协议开源，使用本代码需保留本版权声明。

# API接口文档

## 基础URL
```
http://localhost:5000
```

## 认证方式
所有API请求需要提供有效的API密钥：
```
/api/draw?object=math(sin(x))&api_key=YOUR_API_KEY
```

### 1. 绘图API

**端点**：`GET /api/draw`

**参数**：

- `object`：绘图命令或数学表达式（必需）
- `api_key`：API密钥（必需）
- `width`：图像宽度（可选，默认500）
- `height`：图像高度（可选，默认500）

绘图命令:

| 写法                | 作用                           | 互斥                              |
| ------------------- | ------------------------------ | --------------------------------- |
| `line(x1,y1,x2,y2)` | 画出一条`x1,y1`到`x2,y2`的直线 | `math(f(x))`                      |
| `point(x,y)`        | 在`x,y`处画一个点              | `math(f(x))`                      |
| `math(f(x))`        | 画出函数表达式$f(x)$           | `line(x1,y1,x2,y2)`与`point(x,y)` |

**示例请求**：

```bash
# 绘制正弦函数
curl "http://localhost:5000/api/draw?object=math(sin(x))&api_key=your_api_key"

# 绘制多个函数
curl "http://localhost:5000/api/draw?object=math(sin(x));math(cos(x))&api_key=your_api_key"

# 自定义图像尺寸
curl "http://localhost:5000/api/draw?object=math(log(x+1))&api_key=your_api_key&width=800&height=600"
```

### 2. 计算API

**端点**：`GET /api/calc/<method>`

**支持的计算方法**：

#### 基本运算
- `pow`：幂运算 `x^y`
- `sqrt`：平方根 `√x`
- `log`：对数运算 `logₐb`

#### 三角函数
- `sin`：正弦函数
- `cos`：余弦函数
- `tan`：正切函数
- `arcsin`：反正弦函数
- `arccos`：反余弦函数
- `arctan`：反正切函数

#### 其他数学函数
- `abs`：绝对值
- `floor`：向下取整
- `ceil`：向上取整
- `round`：四舍五入

**示例请求**：
```bash
# 计算 2^10
curl "http://localhost:5000/api/calc/pow?x=2&y=10&api_key=your_api_key"

# 计算 sin(π/2)
curl "http://localhost:5000/api/calc/sin?x=1.5708&api_key=your_api_key"

# 计算 log₂8
curl "http://localhost:5000/api/calc/log?a=2&b=8&api_key=your_api_key"
```

### 3. 数据存储API

**端点**：`GET/POST/DELETE /api/store`

**操作**：
- `GET`：获取指定键的值
- `POST`：设置键值对
- `DELETE`：删除指定键
- `ITEMS`：获取所有键值对
- `CLEAR_EXPIRED`：清理过期数据

**示例请求**：
```bash
# 设置值
curl -X POST "http://localhost:5000/api/store?key=username&value=admin&api_key=your_api_key"

# 获取值
curl "http://localhost:5000/api/store?key=username&api_key=your_api_key"

# 删除值
curl -X DELETE "http://localhost:5000/api/store?key=username&api_key=your_api_key"
```

### 4. 二维码生成API

**端点**：`GET /api/qr_code`

**参数**：
- `content`：二维码内容（必需）
- `size`：二维码尺寸（可选，默认200）

**示例请求**：
```bash
# 生成二维码
curl "http://localhost:5000/api/qr_code?content=Hello%20World&api_key=your_api_key"

# 自定义尺寸
curl "http://localhost:5000/api/qr_code?content=https://example.com&size=300&api_key=your_api_key"
```

### 5. 批量操作API

**端点**：`POST /api/batch`

**参数**：
- `operations`：操作数组（必需，最多50个操作）

**支持的操作类型**：
- `store_get`：获取存储值
- `store_set`：设置存储值
- `calc`：执行计算

**示例请求**：
```bash
curl -X POST "http://localhost:5000/api/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [
      {"type": "store_set", "key": "test1", "value": "value1"},
      {"type": "store_get", "key": "test1"},
      {"type": "calc", "method": "pow", "params": {"x": 2, "y": 10}}
    ]
  }' \
  -H "X-API-Key: your_api_key"
```
