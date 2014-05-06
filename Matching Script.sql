Select temp.*,
       mtch.id_number,
       gft.prim_amt,
       gft.date_of_record,
       gft.alloc_dept_desc,
       b.record_stat_code,
       b.do_not_mail,
       b.do_not_solicit,
       b.do_not_contact
     

  from (select xx.*
          from (select x.*,
                       row_number() over(partition by mr order by JWS desc) match_rank
                  from (select t.MR,
                               t.firstname,
                               t.lastname,
                               b.id_number,
                               b.first_name,
                               b.last_name,
                               a.street1,
                               a.addr_status_code,
                               t.streetaddress,
                               utl_match.jaro_winkler_similarity(lower(trim(t.streetaddress)),
                                                                 lower(a.street1)) jws
                          from ucd_temp_1019030 t,
                               ucdr_bio         b,
                               address          a,
                               name             n

                         where soundex(substr(t.firstname, 1, instr(t.firstname, ' ') -1)) || soundex(t.lastname) =
                               soundex(n.first_name) || soundex(n.last_name)
                           and b.id_number = a.id_number
                           and b.record_stat_code <> 'X'
                           and trim(t.zip) = substr(trim(a.zipcode),1,5)
                           and b.id_number = n.id_number

                        ) x) xx
         where match_rank = 1
           and jws > 84) mtch,
       (select *
          from (select g.id_number,
                       g.prim_amt,
                       g.date_of_record,
                       g.alloc_dept_desc,
                       row_number() over(partition by id_number order by g.date_of_record desc, g.prim_amt) row_rank
                  from ucdr_giving g
                 where g.prim_amt > 0)

         where row_rank = 1) gft,
       ucd_temp_1019030 temp,
       ucdr_bio b

 where temp.mr = mtch.mr(+)
   and mtch.id_number = gft.id_number(+)
   and mtch.id_number = b.id_number(+)
   and mtch.id_number is not null