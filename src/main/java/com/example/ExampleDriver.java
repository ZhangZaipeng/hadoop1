package com.example; /**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import com.example.mapreduce.dept.Q1_SumDeptSalary;
import com.example.mapreduce.dept.Q2_DeptNumberAveSalary;
import com.example.mapreduce.dept.Q5_EarnMoreThanManager;
import com.example.mapreduce.dept.Q9_EmpSalarySort;
import com.example.mapreduce.logs.LoggerTransfrom;
import com.example.mapreduce.WordCount;
import com.example.mapreduce.WordMean;
import org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("wordcount", WordCount.class,
          "A map/reduce program that counts the words in the input files.");
      pgd.addClass("wordmean", WordMean.class,
          "A map/reduce program that counts the average length of the words in the input files.");

      pgd.addClass("Q1_SumDeptSalary", Q1_SumDeptSalary.class, "求各个部门的总工资");

      pgd.addClass("Q2_DeptNumberAveSalary", Q2_DeptNumberAveSalary.class, "求各个部门的人数和平均工资");

      pgd.addClass("Q5_EarnMoreThanManager", Q5_EarnMoreThanManager.class, "列出工资比上司高的员工姓名及其工资");

      pgd.addClass("Q9_EmpSalarySort", Q9_EmpSalarySort.class, "将全体员工按照总收入,从高到低排列");

      pgd.addClass("LoggerTransfrom", LoggerTransfrom.class, "日志文件清洗");

      exitCode = pgd.run(argv);
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);
  }
}
	
