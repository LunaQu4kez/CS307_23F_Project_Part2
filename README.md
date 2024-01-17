# CS307_23F_Project_Part2

CS307 数据库原理2023秋季 Project Part 2

得分 98/100

Project 说明及要求：https://github.com/hezean/sustc



## 小组成员及分工

见 [Report.md](https://github.com/wLUOw/CS307_23F_Project_Part2-sustc-/tree/main/docs/Report.md)



## 项目结构

```
CS307_23F_Project_Part2
├── docs
├── pic
├── sql
│   └── tables.sql
├── sustc-api                                              
│   ├── build.gradle.kts                                   
│   └── src/main/java
│                └── io.sustc
│                    ├── dto                               
│                    │   └── *User.java                    
│                    └── service                           
│                        ├── *Service.java
│                        └── impl                          
│                            └── *ServiceImpl.java
├── sustc-runner                                          
│   ├── compose.yml                                        
│   ├── data                                              
│   └── src/main
│           ├── java
│           │   └── io.sustc
│           │       └── command                            
│           │           └── UserCommand.java
│           └── resources
│               └── application.yml 
├── * (related to gradle)
├── README.md
├── LICENSE
└── .gitignore
```



## 完成任务列表

- [x] 数据库设计

- [x] 建表 sql
- [x] E-R 图
- [x] 完成 DatabaseService 实现类

- [x] 完成 UserService 实现类
- [x] 完成 VideoService 实现类
- [x] 完成 DanmuService 实现类
- [x] 完成 RecommenderService 实现类
- [x] 导入数据多线程优化
- [x] 密码加密存入
- [x] BV 生成算法
