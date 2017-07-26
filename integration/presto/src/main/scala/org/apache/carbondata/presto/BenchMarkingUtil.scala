/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto

import scala.io.Source

object BenchMarkingUtil {

  val queries: Array[Query] = Array(

    Query("select\n        l_returnflag,\n        l_linestatus,\n        sum(l_quantity) as " +
          "sum_qty,\n        sum(l_extendedprice) as sum_base_price,\n        sum(l_extendedprice" +
          " * (1 - l_discount)) as sum_disc_price,\n        sum(l_extendedprice * (1 - " +
          "l_discount) * (1 + l_tax)) as sum_charge,\n        avg(l_quantity) as avg_qty,\n      " +
          "  avg(l_extendedprice) as avg_price,\n        avg(l_discount) as avg_disc,\n        " +
          "count(*) as count_order\nfrom\n        lineitem\nwhere\n        l_shipdate <= cast" +
          "('1998-09-16' as timestamp)\ngroup by\n        l_returnflag,\n        " +
          "l_linestatus\norder by\n        l_returnflag,\n        l_linestatus ","",""),

   Query("select\n        s_acctbal,\n        s_name,\n        n_name,\n        p_partkey,\n    " +
          "    p_mfgr,\n        s_address,\n        s_phone,\n        s_comment\nfrom\n        " +
          "part,\n        supplier,\n        partsupp,\n        nation,\n        region\nwhere\n " +
          "       p_partkey = ps_partkey\n        and s_suppkey = ps_suppkey\n        and p_size " +
          "= 15\n        and p_type like '%BRASS'\n        and s_nationkey = n_nationkey\n       " +
          " and n_regionkey = r_regionkey\n        and r_name = 'EUROPE'\n        and " +
          "ps_supplycost = (\n                select\n                        min(ps_supplycost)" +
          "\n                from\n                        partsupp, supplier,nation, region\n   " +
          "             where\n                        p_partkey = ps_partkey\n                  " +
          "      and s_suppkey = ps_suppkey\n                        and s_nationkey = " +
          "n_nationkey\n                        and n_regionkey = r_regionkey\n                  " +
          "      and r_name = 'EUROPE'\n                )\norder by\n        s_acctbal desc,\n   " +
          "     n_name,\n        s_name,\n        p_partkey\nlimit 100","",""),


    Query("select\n        l_orderkey,\n        sum(l_extendedprice * (1 - l_discount)) as " +
          "revenue,\n        o_orderdate,\n        o_shippriority\nfrom\n        customer,\n     " +
          "   orders,\n        lineitem\nwhere\n        c_mktsegment = 'BUILDING'\n        and " +
          "c_custkey = o_custkey\n        and l_orderkey = o_orderkey\n        and o_orderdate < " +
          " cast('1995-03-22' as timestamp)\n        and l_shipdate > cast('1995-03-22' as " +
          "timestamp)\ngroup by\n        l_orderkey,\n        o_orderdate,\n        " +
          "o_shippriority\norder by\n        revenue desc,\n        o_orderdate\nlimit 10","",""),

    Query("select\n        o_orderpriority,\n        count(*) as order_count\nfrom\n        " +
          "orders as o\nwhere\n        o_orderdate >= cast('1996-05-01' as timestamp)\n        " +
          "and o_orderdate < cast('1996-08-01' as timestamp)\n        and exists (\n             " +
          "   select\n                        *\n                from\n                        " +
          "lineitem\n                where\n                        l_orderkey = o.o_orderkey\n  " +
          "                      and l_commitdate < l_receiptdate\n        )\ngroup by\n        " +
          "o_orderpriority\norder by\n        o_orderpriority","",""),

    Query("select\n        n_name,\n        sum(l_extendedprice * (1 - l_discount)) as " +
          "revenue\nfrom\n        customer,\n        orders,\n        lineitem,\n        " +
          "supplier,\n        nation,\n        region\nwhere\n        c_custkey = o_custkey\n    " +
          "    and l_orderkey = o_orderkey\n        and l_suppkey = s_suppkey\n        and " +
          "c_nationkey = s_nationkey\n        and s_nationkey = n_nationkey\n        and " +
          "n_regionkey = r_regionkey\n        and r_name = 'AFRICA'\n        and o_orderdate >= " +
          "cast('1993-01-01' as timestamp)\n        and o_orderdate < cast('1994-01-01' as " +
          "timestamp)\ngroup by\n        n_name\norder by\n        revenue desc","",""),

    Query("select\n        sum(l_extendedprice * l_discount) as revenue\nfrom\n        " +
          "lineitem\nwhere\n        l_shipdate >= cast('1993-01-01' as timestamp)\n        and " +
          "l_shipdate < cast('1994-01-01' as timestamp) \n        and l_discount between 0.06 - 0" +
          ".01 and 0.06 + 0.01\n        and l_quantity < 25","",""),

    Query("select\n        supp_nation,\n        cust_nation,\n        l_year,\n        sum" +
          "(volume) as revenue\nfrom\n        (\n                select\n                        " +
          "n1.n_name as supp_nation,\n                        n2.n_name as cust_nation,\n        " +
          "                year(l_shipdate) as l_year,\n                        l_extendedprice *" +
          " (1 - l_discount) as volume\n                from\n                        supplier,\n" +
          "                        lineitem,\n                        orders,\n                  " +
          "      customer,\n                        nation n1,\n                        nation " +
          "n2\n                where\n                        s_suppkey = l_suppkey\n            " +
          "            and o_orderkey = l_orderkey\n                        and c_custkey = " +
          "o_custkey\n                        and s_nationkey = n1.n_nationkey\n                 " +
          "       and c_nationkey = n2.n_nationkey\n                        and (\n              " +
          "                  (n1.n_name = 'KENYA' and n2.n_name = 'PERU')\n                      " +
          "          or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')\n                        )\n" +
          "                        and l_shipdate between cast('1995-01-01' as timestamp) and " +
          "cast('1996-12-31' as timestamp)\n        ) as shipping\ngroup by\n        supp_nation," +
          "\n        cust_nation,\n        l_year\norder by\n        supp_nation,\n        " +
          "cust_nation,\n        l_year","",""),


    Query("select\n        o_year,\n        sum(case\n            when nation = 'BRAZIL'\n       " +
          "     then volume\n            else  0\n            end) / sum(volume) as " +
          "mkt_share\nfrom\n        (select\n                 year(o_orderdate) as o_year,\n     " +
          "            l_extendedprice * (1-l_discount) as volume,\n                 n2.n_name as" +
          " nation\n        from\n                part,supplier,lineitem," +
          "orders,customer,nation n1,nation n2,region\n        where\n                " +
          "p_partkey = l_partkey\n                and s_suppkey = l_suppkey\n                and " +
          "l_orderkey = o_orderkey\n                and o_custkey = c_custkey\n                " +
          "and c_nationkey = n1.n_nationkey\n                and n1.n_regionkey = r_regionkey\n  " +
          "              and r_name = 'AMERICA'\n                and s_nationkey = n2" +
          ".n_nationkey\n                and o_orderdate between cast('1995-01-01' as timestamp) and cast('1996-12-31' as timestamp)\n " +
          "               and p_type = 'ECONOMY ANODIZED STEEL'\n        ) as all_nations\ngroup " +
          "by\n        o_year\norder by\n        o_year","",""),

    Query("select\n        nation,\n        o_year,\n        sum(amount) as sum_profit\nfrom\n   " +
          "     (\n                select \n                        n_name as nation,\n          " +
          "              year(o_orderdate) as o_year,\n                        l_extendedprice * " +
          "(1 - l_discount) - ps_supplycost * l_quantity as amount\n                from\n       " +
          "                 part,\n                        supplier,\n                        " +
          "lineitem,\n                        partsupp,\n                        " +
          "orders,\n                        nation\n                where\n            " +
          "            s_suppkey = l_suppkey\n                        and ps_suppkey = " +
          "l_suppkey\n                        and ps_partkey = l_partkey\n                       " +
          " and p_partkey = l_partkey\n                        and o_orderkey = l_orderkey\n     " +
          "                   and s_nationkey = n_nationkey\n                        and p_name " +
          "like '%green%'\n        ) as profit\ngroup by\n        nation,\n        o_year\norder " +
          "by\n        nation,\n        o_year desc","",""),

    Query("select\n        c_custkey,\n        c_name,\n        sum(l_extendedprice * (1 - " +
          "l_discount)) as revenue,\n        c_acctbal,\n        n_name,\n        c_address,\n   " +
          "     c_phone,\n        c_comment\nfrom\n        customer,\n        orders,\n        " +
          "lineitem,\n        nation\nwhere\n        c_custkey = o_custkey\n        and " +
          "l_orderkey = o_orderkey\n        and o_orderdate >= cast('1993-07-01' as timestamp)\n " +
          "       and o_orderdate < cast('1993-10-01' as timestamp)\n        and l_returnflag = " +
          "'R'\n        and c_nationkey = n_nationkey\ngroup by\n        c_custkey,\n        " +
          "c_name,\n        c_acctbal,\n        c_phone,\n        n_name,\n        c_address,\n  " +
          "      c_comment\norder by\n        revenue desc\nlimit 20","",""),

    Query("select\n        ps_partkey,\n        sum(ps_supplycost * ps_availqty) as value\nfrom\n" +
          "        partsupp,\n        supplier,\n        nation\nwhere\n        ps_suppkey = " +
          "s_suppkey\n        and s_nationkey = n_nationkey\n        and n_name = " +
          "'GERMANY'\ngroup by\n        ps_partkey having\n        sum(ps_supplycost * " +
          "ps_availqty) > (\n                select\n                        sum(ps_supplycost * " +
          "ps_availqty) * 0.0001000000 s\n                from\n                        partsupp," +
          "\n                        supplier,\n                        nation\n                " +
          "where\n                        ps_suppkey = s_suppkey\n                        and " +
          "s_nationkey = n_nationkey\n                        and n_name = 'GERMANY'\n        )" +
          "\norder by\n        value desc","",""),

    Query("select\n        l_shipmode,\n        sum(case\n                when o_orderpriority = " +
          "'1-URGENT'\n                        or o_orderpriority = '2-HIGH'\n                   " +
          "     then 1\n                else 0\n        end) as high_line_count,\n        sum" +
          "(case\n                when o_orderpriority <> '1-URGENT'\n                        and" +
          " o_orderpriority <> '2-HIGH'\n                        then 1\n                else 0\n" +
          "        end) as low_line_count\nfrom\n        orders,\n        lineitem\nwhere\n      " +
          "  o_orderkey = l_orderkey\n        and l_shipmode in ('REG AIR', 'MAIL')\n        and " +
          "l_commitdate < l_receiptdate\n        and l_shipdate < l_commitdate\n        and " +
          "l_receiptdate >= cast('1995-01-01' as timestamp) \n        and l_receiptdate < cast" +
          "('1996-01-01' as timestamp) \ngroup by\n        l_shipmode\norder by\n        l_shipmode","",""),

    Query("select\n        c_count, count(*) as custdist\nfrom \n        (select\n               " +
          " c_custkey,\n                count(o_orderkey) as c_count\n        from\n             " +
          "   customer left outer join orders on (\n                        c_custkey =" +
          " o_custkey\n                        and o_comment not like '%special%requests%'\n     " +
          "           )\n        group by\n                c_custkey\n        ) as " +
          "c_orders\ngroup by\n        c_count\norder by\n        custdist desc,\n        c_count" +
          " desc","",""),

    Query(" \nselect\n   " +
          "     100.00 * sum(case\n                when p_type like 'PROMO%'\n                   " +
          "     then l_extendedprice * (1 - l_discount)\n                else 0\n        end) / " +
          "sum(l_extendedprice * (1 - l_discount)) as promo_revenue\nfrom\n        lineitem,\n   " +
          "     part\nwhere\n        l_partkey = p_partkey\n        and l_shipdate >= cast" +
          "('1995-08-01' as timestamp) \n        and l_shipdate < cast('1995-09-01' as timestamp)","",""),

    Query("select\n        p_brand,\n        p_type,\n        p_size,\n        count(distinct " +
          "ps_suppkey) as supplier_cnt\nfrom\n        partsupp,\n        part\nwhere\n        " +
          "p_partkey = ps_partkey\n        and p_brand <> 'Brand#34'\n        and p_type not like" +
          " 'ECONOMY BRUSHED%'\n        and p_size in (22, 14, 27, 49, 21, 33, 35, 28)\n        " +
          "and partsupp.ps_suppkey not in (\n                select\n                        " +
          "s_suppkey\n                from\n                        supplier\n                " +
          "where\n                        s_comment like '%Customer%Complaints%'\n        )" +
          "\ngroup by\n        p_brand,\n        p_type,\n        p_size\norder by\n        " +
          "supplier_cnt desc,\n        p_brand,\n        p_type,\n        p_size","",""),

    Query("select\n        sum(l_extendedprice) / 7.0 as avg_yearly\nfrom\n        " +
          "lineitem,part\nwhere\n        p_partkey = l_partkey\n        and p_brand = " +
          "'Brand#23'\n        and p_container = 'MED BOX'\n        and l_quantity < (\n         " +
          "       select\n                        0.2 * avg(l_quantity)\n                from\n  " +
          "                      lineitem\n                where\n                     " +
          "   l_partkey = p_partkey\n        )","",""),

    Query("select\n        c_name,\n        c_custkey,\n        o_orderkey,\n        o_orderdate," +
          "\n        o_totalprice,\n        sum(l_quantity)\nfrom\n        customer,\n        " +
          "orders,\n        lineitem\nwhere\n        o_orderkey in (\n       " +
          " select\n                l_orderkey\n        from\n                " +
          "lineitem\n        group by\n                l_orderkey having\n             " +
          "   sum(l_quantity) > 300\n                )\n        and c_custkey = o_custkey\n      " +
          "  and o_orderkey = l_orderkey\ngroup by\n        c_name,\n        c_custkey,\n        " +
          "o_orderkey,\n        o_orderdate,\n        o_totalprice\norder by\n        " +
          "o_totalprice desc,\n        o_orderdate","",""),

    Query("select\n        sum(l_extendedprice* (1 - l_discount)) as revenue\nfrom\n        " +
          "lineitem,\n        part\nwhere\n        (\n                p_partkey = " +
          "l_partkey\n                and p_brand = 'Brand#12'\n                and p_container " +
          "in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n                and l_quantity >= 1 and" +
          " l_quantity <= 1 + 10\n                and p_size between 1 and 5\n                and" +
          " l_shipmode in ('AIR', 'AIR REG')\n                and l_shipinstruct = 'DELIVER IN " +
          "PERSON'\n        )\n        or\n        (\n                p_partkey = l_partkey\n    " +
          "            and p_brand = 'Brand#23'\n                and p_container in ('MED BAG', " +
          "'MED BOX', 'MED PKG', 'MED PACK')\n                and l_quantity >= 10 and l_quantity" +
          " <= 10 + 10\n                and p_size between 1 and 10\n                and " +
          "l_shipmode in ('AIR', 'AIR REG')\n                and l_shipinstruct = 'DELIVER IN " +
          "PERSON'\n        )\n        or\n        (\n                p_partkey = l_partkey\n    " +
          "            and p_brand = 'Brand#34'\n                and p_container in ('LG CASE', " +
          "'LG BOX', 'LG PACK', 'LG PKG')\n                and l_quantity >= 20 and l_quantity <=" +
          " 20 + 10\n                and p_size between 1 and 15\n                and l_shipmode " +
          "in ('AIR', 'AIR REG')\n                and l_shipinstruct = 'DELIVER IN PERSON'\n     " +
          "   )","",""),

    Query("select\n        s_name,\n        s_address\nfrom\n        supplier, nation\nwhere\n   " +
          "     s_suppkey in (\n                select\n                        ps_suppkey\n     " +
          "           from\n                        partsupp\n                where\n            " +
          "            ps_partkey in (\n                                select\n                 " +
          "                       p_partkey\n                                from\n              " +
          "                          part\n                                where\n               " +
          "                         p_name like 'forest%'\n                        )\n           " +
          "             and ps_availqty > (\n                                select\n            " +
          "                            0.5 * sum(l_quantity)\n                                " +
          "from\n                                        lineitem\n                    " +
          "            where\n                                        l_partkey = ps_partkey\n   " +
          "                                     and l_suppkey = ps_suppkey\n                     " +
          "                   and l_shipdate >= cast('1994-01-01' as timestamp)\n                                   " +
          "     and l_shipdate < cast('1995-01-01' as timestamp)\n                        )\n        )\n        and " +
          "s_nationkey = n_nationkey\n        and n_name = 'CANADA'\norder by\n        s_name","",""),

    Query("select\n        s_name,\n        count(*) as numwait\nfrom\n        supplier,\n       " +
          " lineitem l1,\n        orders,\n        nation\nwhere\n        " +
          "s_suppkey = l1.l_suppkey\n        and o_orderkey = l1.l_orderkey\n        and " +
          "o_orderstatus = 'F'\n        and l1.l_receiptdate > l1.l_commitdate\n        and " +
          "exists (\n                select\n                        *\n                from\n   " +
          "                     lineitem l2\n                where\n                   " +
          "     l2.l_orderkey = l1.l_orderkey\n                        and l2.l_suppkey <> l1" +
          ".l_suppkey\n                )\n        and not exists (\n                select\n     " +
          "                   *\n                from\n                        lineitem" +
          " l3\n                where\n                        l3.l_orderkey = l1.l_orderkey\n   " +
          "                     and l3.l_suppkey <> l1.l_suppkey\n                        and l3" +
          ".l_receiptdate > l3.l_commitdate\n        )\n        and s_nationkey = n_nationkey\n  " +
          "      and n_name = 'SAUDI ARABIA'\ngroup by\n        s_name\norder by\n        numwait" +
          " desc,\n        s_name","","")
  )


  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  def writeResults(content: String, file: String): Unit = {
    scala.tools.nsc.io.File(file).appendAll(content)
  }

  def readFromFile(file: String): List[String] = {
    Source.fromFile(file).getLines.toList
  }
}