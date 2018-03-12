CREATE OR REPLACE PACKAGE BODY pkg_parsing_swift_file is

  procedure get_dir_list(p_directory in varchar2) as
    language java name 'SW_DirList.getList( java.lang.String )';
  
 procedure load_file as
   PRAGMA AUTONOMOUS_TRANSACTION;
   v_clob     clob;
   v_bfile    bfile;
   dst_offset number := 1;
   src_offset number := 1;
   lang_ctx   number := DBMS_LOB.DEFAULT_LANG_CTX;
   warning    number;
 begin
 
   delete from file_dir_list;
 
   get_dir_list(c_directory);
 
   for i in (select * from file_dir_list t where t.filename <> 'archive')
   
    loop
   
     insert into TMP_SWIFT_FILE
       (FILE_BODY, FILE_ID, STATUS, MSG_TYPE, PARENT_FILE_ID, FILE_NAME)
     values
       (empty_clob(), null, 0, null, null, i.filename)
     returning FILE_BODY into v_clob;
   
     v_bfile := bfilename(c_directory1, i.filename);
   
     dbms_lob.fileopen(v_bfile);
   
     DBMS_LOB.LoadCLOBFromFile(DEST_LOB     => v_clob,
                               SRC_BFILE    => v_bfile,
                               AMOUNT       => DBMS_LOB.GETLENGTH(v_bfile),
                               DEST_OFFSET  => dst_offset,
                               SRC_OFFSET   => src_offset,
                               BFILE_CSID   => DBMS_LOB.DEFAULT_CSID,
                               LANG_CONTEXT => lang_ctx,
                               WARNING      => warning);
   
     dbms_lob.fileclose(v_bfile);
     v_clob := empty_clob();
     commit;
   end loop;
 
 end;
  
 procedure get_102_ex_data(p_file_id        in number,
                           p_SENDER         out varchar2,
                           p_BANK_OPER_CODE out varchar2,
                           p_VALUE_DATE     out date,
                           p_PACKAGE_REF_NO   OUT VARCHAR2) as
 
 begin
   select 
   
   case when instr(t1.file_body,'<msg_sender>')>0
     then substr(t1.file_body,instr(t1.file_body,'<msg_sender>')+12,8)
    else  substr(t1.file_body, 103, 8)
   end sender, substr(t1.file_body, instr(t1.file_body, ':23:') + 4, 6),
          
          to_date(substr(t1.file_body, instr(t1.file_body, ':32A:') + 5, 6),
                  'YYMMDD'),
         replace(REPLACE(substr(t1.file_body,
                      instr(t1.file_body, ':20:') + 4,
                      INSTR(substr(t1.file_body, instr(t1.file_body, ':20:')),
                            CHR(10)) - 4),
               CHR(10),
               ''),chr(13),'')      
                  
     into p_SENDER, p_BANK_OPER_CODE, p_VALUE_DATE, p_PACKAGE_REF_NO
     from TMP_SWIFT_FILE t, TMP_SWIFT_FILE T1
    where t.file_id = p_file_id
      and t1.file_id = t.parent_file_id;
 
 exception
   when others then
     null;
 end;

  function get_tag(p_tag in varchar2) return boolean is
    v_count number := 0;
  begin
    select count(*)
      into v_count
      from SWIFT_FORMAT t
     where t.field = replace(p_tag, ':', '');
    if v_count > 0 then
      return true;
    else
      return false;
    end if;
  
  end;

  procedure ins_tag(v_file_id in number, v_tag_val in varchar2) as
  begin
    insert into swift_file_tags
      (file_id, file_tags)
    values
      (v_file_id, v_tag_val);
  end;

  procedure set_msg_type as
  PRAGMA AUTONOMOUS_TRANSACTION;
  v_file_body clob;
  v_msg_type varchar2(10);
  v_income_type varchar2(20);
  begin
    for i in (select t.* from TMP_SWIFT_FILE t where t.msg_type is null)
      loop
        v_file_body := i.file_body;
		v_income_type := 'SWIFT'; 
		
		/*  Some line removed. You can add for check Message source (local bank, Swift or ect. */
               
        update TMP_SWIFT_FILE t set t.msg_type=v_msg_type, t.income_type=v_income_type where t.file_id=i.file_id;
        commit;
      end loop;
  end;  
  
  procedure MT102_TO_MT103 as
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_offset    NUMBER := 1;
    v_amount    NUMBER;
    v_length    NUMBER;
    v_buffer    VARCHAR2(32767) := '';
    v_file_body clob;
    v_tag_body  varchar2(32767) := '';
  begin
    for i in (select t.file_id, t.file_body, t.income_type
                from TMP_SWIFT_FILE t
               where t.msg_type in ('102','150')  /* Some files include more than one 103 file. Add comma separate message types.  */
                 and t.status = 0) loop
    
      v_file_body := i.file_body;
    
      v_length := DBMS_LOB.getLength(v_file_body);
      v_offset :=1; 
      v_amount := 0;
      v_tag_body :='';
      
      WHILE v_offset < v_length LOOP
        v_amount := LEAST(DBMS_LOB.instr(v_file_body, chr(10), v_offset) -
                          v_offset,
                          32767);
        IF v_amount > 0 THEN
          DBMS_LOB.read(v_file_body, v_amount, v_offset, v_buffer);
          v_offset := v_offset + v_amount + 1;
        ELSE
          v_buffer := NULL;
          v_offset := v_offset + 1;
        END IF;
      
        if instr(v_buffer, ':21:') > 0 or instr(v_buffer, ':32A:') > 0 then
        
          if instr(v_tag_body, ':21:') > 0 then
            insert into TMP_SWIFT_FILE
              (FILE_BODY,
               FILE_ID,
               STATUS,
               MSG_TYPE,
               PARENT_FILE_ID,
               income_type)
            values
              (v_tag_body, null, 0, '103', i.file_id, i.income_type);
            commit;
          end if;
        
          v_tag_body := '';
        end if;
      
        v_tag_body := v_tag_body || v_buffer || chr(10);
      
      END LOOP;
    
      update TMP_SWIFT_FILE t set t.status = 1 where t.file_id = i.file_id;
      commit;
    end loop;
  end;
  
  procedure processing_file(v_file_id in number) as
    v_offset    NUMBER := 1;
    v_amount    NUMBER;
    v_length    NUMBER;
    v_buffer    VARCHAR2(32767) := '';
    v_file_body clob;
    v_tag_exi   boolean;
    v_tag_body  varchar2(32767) := '';
  
  begin
  
    select t.file_body
      into v_file_body
      from TMP_SWIFT_FILE t
     where t.file_id = v_file_id
       and t.status = 0;
  
    v_length := DBMS_LOB.getLength(v_file_body);
  
    WHILE v_offset < v_length LOOP
      v_amount := LEAST(DBMS_LOB.instr(v_file_body, chr(10), v_offset) -
                        v_offset,
                        32767);
      IF v_amount > 0 THEN
        DBMS_LOB.read(v_file_body, v_amount, v_offset, v_buffer);
        v_offset := v_offset + v_amount + 1;
      ELSE
        v_buffer := NULL;
        v_offset := v_offset + 1;
      END IF;
    
      v_tag_exi := get_tag(substr(v_buffer, 1, 4));
    
      if v_tag_exi then
        ins_tag(v_file_id => v_file_id, v_tag_val => v_tag_body);
        v_tag_body := '';
      end if;
    
      v_tag_body := v_tag_body || v_buffer || chr(10);
    
    END LOOP;
  
    ins_tag(v_file_id => v_file_id, v_tag_val => v_tag_body);
  
    update TMP_SWIFT_FILE t set t.status = 1 where t.file_id = v_file_id;
  end;

  procedure processing_tag(v_file_id in number) as
    t_swift_type    swift_type;
    v_tag_name      varchar2(5);
    v_tag_val       varchar2(3000);
    v_range_count   number := 0;
    ty_FTTBS_UPLOAD t_FTTBS_UPLOAD;
    v_amount1 varchar2(50);
    v_amount2 varchar2(50);
    v_amount3 varchar2(50);
    v_amount4 number; 
  
  begin
  
    ty_FTTBS_UPLOAD := t_FTTBS_UPLOAD();
    ty_FTTBS_UPLOAD.EXTEND(1);
  
    FOR E IN (select DISTINCT DEST_FIELD from SWIFT_FORMAT) LOOP
      t_swift_type(E.dest_field) := '';
    END LOOP;
  
    for i in (select * from swift_file_tags t where t.file_id = v_file_id) loop
    
      v_tag_val  := i.file_tags;
      v_tag_name := replace(substr(v_tag_val, 1, 4), ':');
    
      for j in (select *
                  from SWIFT_FORMAT x
                 where x.field = v_tag_name
                 order by x.sequence) loop
      
        select decode(j.range_count,
                      0,
                      instr(v_tag_val, chr(10)) - j.range_from + 1,
                      j.range_count)
          into v_range_count
          from dual;
      
        t_swift_type(j.dest_field) := substr(v_tag_val,
                                             j.range_from,
                                             v_range_count);
      
        v_tag_val := substr(v_tag_val,
                            length(t_swift_type(j.dest_field)) +
                            j.range_from);
      
      end loop;
    end loop;
  
    ty_FTTBS_UPLOAD(1).USER_REF_NO := replace(replace(t_swift_type('USER_REF_NO'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).BANK_OPER_CODE := replace(replace(t_swift_type('BANK_OPER_CODE'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).VALUE_DATE := TO_DATE(replace(replace(t_swift_type('VALUE_DATE'),chr(10),''),chr(13),''),'YYMMDD');
    ty_FTTBS_UPLOAD(1).DR_CCY := replace(replace(t_swift_type('DR_CCY'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).DR_AMOUNT := to_number(replace(REPLACE(replace(t_swift_type('DR_AMOUNT'),chr(10),''),',','.'),chr(13),''));
    ty_FTTBS_UPLOAD(1).SENDER := replace(replace(t_swift_type('SENDER'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).BY_ORDER_OF1 := replace(replace(t_swift_type('BY_ORDER_OF1'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).BY_ORDER_OF2 := replace(replace(t_swift_type('BY_ORDER_OF2'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).BY_ORDER_OF3 := replace(replace(t_swift_type('BY_ORDER_OF3'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).BY_ORDER_OF4 := replace(replace(t_swift_type('BY_ORDER_OF4'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).BY_ORDER_OF5 := replace(replace(t_swift_type('BY_ORDER_OF5'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).ULTIMATE_BEN1 := replace(replace(t_swift_type('ULTIMATE_BEN1'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).ULTIMATE_BEN2 := replace(replace(t_swift_type('ULTIMATE_BEN2'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).ULTIMATE_BEN3 := replace(replace(t_swift_type('ULTIMATE_BEN3'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).ULTIMATE_BEN4 := replace(replace(t_swift_type('ULTIMATE_BEN4'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).ULTIMATE_BEN5 := replace(replace(t_swift_type('ULTIMATE_BEN5'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS1 := replace(replace(t_swift_type('PAYMENT_DETAILS1'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS2 := replace(replace(t_swift_type('PAYMENT_DETAILS2'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS3 := replace(replace(t_swift_type('PAYMENT_DETAILS3'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS4 := replace(replace(t_swift_type('PAYMENT_DETAILS4'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).INFORMATION1 := replace(replace(t_swift_type('INFORMATION1'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).INFORMATION2 := replace(replace(t_swift_type('INFORMATION2'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).INFORMATION3 := replace(replace(t_swift_type('INFORMATION3'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).INFORMATION4 := replace(replace(t_swift_type('INFORMATION4'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).INFORMATION5 := replace(replace(t_swift_type('INFORMATION5'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).SENDER53 := replace(replace(t_swift_type('SENDER53'),chr(10),''),chr(13),'');
    ty_FTTBS_UPLOAD(1).SENDER54 := replace(replace(t_swift_type('SENDER54'),chr(10),''),chr(13),'');

    
    if ty_FTTBS_UPLOAD(1).VALUE_DATE is null then
      
      get_102_ex_data(p_file_id        => v_file_id,
                      p_SENDER         => ty_FTTBS_UPLOAD(1).SENDER,
                      p_BANK_OPER_CODE => ty_FTTBS_UPLOAD(1).BANK_OPER_CODE,
                      p_VALUE_DATE     => ty_FTTBS_UPLOAD(1).VALUE_DATE,
                      p_PACKAGE_REF_NO => ty_FTTBS_UPLOAD(1).PACKAGE_REF_NO);
    end if;
  
  
    INSERT INTO swift_PAYMENTS_table
      (ft_contract_ref,
       user_ref_no,
       bank_oper_code,
       value_date,
       dr_ccy,
       dr_amount,
       SENDER,
       by_order_of1,
       by_order_of2,
       by_order_of3,
       by_order_of4,
       by_order_of5,
       ultimate_ben1,
       ultimate_ben2,
       ultimate_ben3,
       ultimate_ben4,
       ultimate_ben5,
       payment_details1,
       payment_details2,
       payment_details3,
       payment_details4,
       INFORMATION1,
       INFORMATION2,
       INFORMATION3,
       INFORMATION4,
       INFORMATION5,
       FILE_ID,
       PACKAGE_REF_NO,
       SENDER53,
       SENDER54)
    VALUES
      (ty_FTTBS_UPLOAD(1).FT_CONTRACT_REF,
       ty_FTTBS_UPLOAD(1).USER_REF_NO,
       ty_FTTBS_UPLOAD(1).BANK_OPER_CODE,
       ty_FTTBS_UPLOAD(1).VALUE_DATE,
       ty_FTTBS_UPLOAD(1).DR_CCY,
       ty_FTTBS_UPLOAD(1).DR_AMOUNT,
       ty_FTTBS_UPLOAD(1).SENDER,
       ty_FTTBS_UPLOAD(1).BY_ORDER_OF1,
       ty_FTTBS_UPLOAD(1).BY_ORDER_OF2,
       ty_FTTBS_UPLOAD(1).BY_ORDER_OF3,
       ty_FTTBS_UPLOAD(1).BY_ORDER_OF4,
       ty_FTTBS_UPLOAD(1).BY_ORDER_OF5,
       ty_FTTBS_UPLOAD(1).ULTIMATE_BEN1,
       ty_FTTBS_UPLOAD(1).ULTIMATE_BEN2,
       ty_FTTBS_UPLOAD(1).ULTIMATE_BEN3,
       ty_FTTBS_UPLOAD(1).ULTIMATE_BEN4,
       ty_FTTBS_UPLOAD(1).ULTIMATE_BEN5,
       ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS1,
       ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS2,
       ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS3,
       ty_FTTBS_UPLOAD(1).PAYMENT_DETAILS4,
       ty_FTTBS_UPLOAD(1).INFORMATION1,
       ty_FTTBS_UPLOAD(1).INFORMATION2,
       ty_FTTBS_UPLOAD(1).INFORMATION3,
       ty_FTTBS_UPLOAD(1).INFORMATION4,
       ty_FTTBS_UPLOAD(1).INFORMATION5,
       v_file_id,
       ty_FTTBS_UPLOAD(1).PACKAGE_REF_NO,
       ty_FTTBS_UPLOAD(1).SENDER53,
       ty_FTTBS_UPLOAD(1).SENDER54);
  
  exception when others then 
    update TMP_SWIFT_FILE t set t.status = 999 where t.file_id = v_file_id;
  end;
 
  procedure processing_parsing_file as
  v_time_level number(2);
  begin    
    
  
    set_msg_type;
    
    MT102_TO_MT103;
    
    FOR I IN (select t.* from TMP_SWIFT_FILE t WHERE T.STATUS = 0 and t.msg_type='103') LOOP
      --dbms_output.put_line(i.file_id);
      processing_file(I.FILE_ID);
      processing_tag(I.FILE_ID);
      commit;
    END LOOP;
    
 
   commit;
  end;

end pkg_parsing_swift_file;
/