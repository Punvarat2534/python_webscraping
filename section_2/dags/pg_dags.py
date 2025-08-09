from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.dag import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='postgres_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    create_table_task = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='pg-1f2e1a3e',  # Replace with your PostgreSQL connection ID
        sql="""
            CREATE TABLE IF NOT EXISTS research (
                id SERIAL PRIMARY KEY,
                title TEXT,
                author TEXT,
                years INTEGER,
                data_resource VARCHAR(255),
                university TEXT
            );
        """,
    )

    insert_data_task = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='pg-1f2e1a3e',
        sql="""
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่มีอิทธิพลต่อการมีผลงานตีพิมพ์ในวารสารทางวิชาการระดับนานาชาติ ของบุคลากรสายวิชาการ คณะศิลปศาสตร์ มหาวิทยาลัยสงขลานครินทร์','สุชาดา กองสวัสดิ์',2024,'TCI','สงขลานหครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การศึกษาประสิทธิภาพของการใช้ระบบฐานข้อมูลผู้เชี่ยวชาญอาจารย์ คณะแพทยศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ณัฎฐา ศิริรักษ์',2024,'TCI','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบยื่นขอกำหนดตำแหน่งทางวิชาการ คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ศิริพร เทพอรัญ',2020,'TCI','วลัยลักษณ์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่มีผลต่ออัลวะสะฏียะฮ์ของนักศึกษาคณะวิทยาการอิสลาม มหาวิทยาลัยสงขลานครินทร์','มูหัมมัดรอฟลี แวหะมะ',2021,'TCI','วลัยลักษณ์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('พฤติกรรมการใช้บริการ และสภาพปัญหา ที่มีอิทธิพลต่อแรงจูงใจของโรงพยาบาลสัตว์ คณะสัตวแพทยศาสตร์ มหาวิทยาลัยสงขลานครินทร์ จังหวัดสงขลา','สุชาดา กองสวัสดิ์',2025,'TCI','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบบริหารจัดการความร่วมมือด้านต่างประเทศและเครือข่าย คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ศิริพร เทพอรัญ',2022,'TCI','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ความรู้ ทัศนคติ และพฤติกรรมการออกกำลังกายของนักศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','วิษณุ โรจน์สุวรรณ',2022,'TCI','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบบริหารจัดการความร่วมมือด้านต่างประเทศและเครือข่าย คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','อัญชลี พัฒนพันธ์ชัย',2024,'TCI','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่ส่งผลต่อผลิตภาพในการทำวิจัยของอาจารย์ในสายสังคมศาสตร์: กรณีศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','สุชาดา กองสวัสดิ์',2025,'TCI','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('แนวทางการพัฒนาคุณภาพระดับหลักสูตรตามเกณฑ์ AUN-QA คณะศิลปศาสตร์ มหาวิทยาลัยสงขลานครินทร์','สุชาดา กองสวัสดิ์',2025,'TCI','ราชภัฏสวนดุสิต');

            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่มีอิทธิพลต่อการมีผลงานตีพิมพ์ในวารสารทางวิชาการระดับนานาชาติ ของบุคลากรสายวิชาการ คณะศิลปศาสตร์ มหาวิทยาลัยสงขลานครินทร์','สุชาดา กองสวัสดิ์',2021,'Scopus','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การศึกษาประสิทธิภาพของการใช้ระบบฐานข้อมูลผู้เชี่ยวชาญอาจารย์ คณะแพทยศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ณัฎฐา ศิริรักษ์',2022,'Scopus','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบยื่นขอกำหนดตำแหน่งทางวิชาการ คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ปริยา อาห์ลุวาเลีย',2025,'Scopus','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่มีผลต่ออัลวะสะฏียะฮ์ของนักศึกษาคณะวิทยาการอิสลาม มหาวิทยาลัยสงขลานครินทร์','วิษณุพงษ์ โพธิพิรุฬห์',2024,'Scopus','วลัยลักษณ์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('พฤติกรรมการใช้บริการ และสภาพปัญหา ที่มีอิทธิพลต่อแรงจูงใจของโรงพยาบาลสัตว์ คณะสัตวแพทยศาสตร์ มหาวิทยาลัยสงขลานครินทร์ จังหวัดสงขลา','บรรณกร แซ่ลิ่ม',2023,'Scopus','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบบริหารจัดการความร่วมมือด้านต่างประเทศและเครือข่าย คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ศิริพร เทพอรัญ',2022,'Scopus','วลัยลักษณ์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ความรู้ ทัศนคติ และพฤติกรรมการออกกำลังกายของนักศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','วิษณุ โรจน์สุวรรณ',2021,'Scopus','วลัยลักษณ์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบบริหารจัดการความร่วมมือด้านต่างประเทศและเครือข่าย คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','กมลวรรณ แสงทอง',2020,'Scopus','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่ส่งผลต่อผลิตภาพในการทำวิจัยของอาจารย์ในสายสังคมศาสตร์: กรณีศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','ธนพล กิ่งภูเขา',2020,'Scopus','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การศึกษาความพร้อมของสถานประกอบการ ตามแนวทางสหกิจศึกษาและการศึกษาเชิงบูรณาการกับการทำงาน ในมุมมองอาจารย์นิเทศก์และนักศึกษา: กรณีศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตภูเก็ต','ธนพล กิ่งภูเขา',2022,'Scopus','ราชภัฏสวนดุสิต');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่ส่งผลต่อผลิตภาพในการทำวิจัยของอาจารย์ในสายสังคมศาสตร์: กรณีศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','ธนพล กิ่งภูเขา',2021,'Scopus','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบบริหารจัดการความร่วมมือด้านต่างประเทศและเครือข่าย คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ธนพล กิ่งภูเขา',2020,'Scopus','สงขลานครินทร์');

            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่มีอิทธิพลต่อการมีผลงานตีพิมพ์ในวารสารทางวิชาการระดับนานาชาติ ของบุคลากรสายวิชาการ คณะศิลปศาสตร์ มหาวิทยาลัยสงขลานครินทร์','เครือมาส แก้วทอน',2021,'OpenAlex','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การศึกษาประสิทธิภาพของการใช้ระบบฐานข้อมูลผู้เชี่ยวชาญอาจารย์ คณะแพทยศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ณัฎฐา ศิริรักษ์',2022,'OpenAlex','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบยื่นขอกำหนดตำแหน่งทางวิชาการ คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','เครือมาส แก้วทอน',2025,'OpenAlex','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่มีผลต่ออัลวะสะฏียะฮ์ของนักศึกษาคณะวิทยาการอิสลาม มหาวิทยาลัยสงขลานครินทร์','เครือมาส แก้วทอน',2024,'OpenAlex','วลัยลักษณ์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('พฤติกรรมการใช้บริการ และสภาพปัญหา ที่มีอิทธิพลต่อแรงจูงใจของโรงพยาบาลสัตว์ คณะสัตวแพทยศาสตร์ มหาวิทยาลัยสงขลานครินทร์ จังหวัดสงขลา','บรรณกร แซ่ลิ่ม',2023,'OpenAlex','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบบริหารจัดการความร่วมมือด้านต่างประเทศและเครือข่าย คณะวิทยาศาสตร์ มหาวิทยาลัยสงขลานครินทร์','ศิริพร เทพอรัญ',2022,'OpenAlex','ธรรมศาสตร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ความรู้ ทัศนคติ และพฤติกรรมการออกกำลังกายของนักศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','เครือมาส แก้วทอน',2021,'OpenAlex','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('แนวทางการพัฒนาผลลัพธ์ด้านการเรียนรู้ของผู้เรียนตามเกณฑ์คุณภาพการศึกษา เพื่อการดำเนินการที่เป็นเลิศ (EdPEx) คณะศิลปศาสตร์ มหาวิทยาลัยสงขลานครินทร์','กมลวรรณ แสงทอง',2020,'OpenAlex','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่ส่งผลต่อผลิตภาพในการทำวิจัยของอาจารย์ในสายสังคมศาสตร์: กรณีศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','เครือมาส แก้วทอน',2020,'OpenAlex','สงขลานครินทร์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การศึกษาความพร้อมของสถานประกอบการ ตามแนวทางสหกิจศึกษาและการศึกษาเชิงบูรณาการกับการทำงาน ในมุมมองอาจารย์นิเทศก์และนักศึกษา: กรณีศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตภูเก็ต','ธนพล กิ่งภูเขา',2022,'OpenAlexs','ราชภัฏสวนดุสิต');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('ปัจจัยที่ส่งผลต่อผลิตภาพในการทำวิจัยของอาจารย์ในสายสังคมศาสตร์: กรณีศึกษา มหาวิทยาลัยสงขลานครินทร์ วิทยาเขตหาดใหญ่','เครือมาส แก้วทอน',2021,'OpenAlex','วลัยลักษณ์');
            INSERT INTO research(title,author,years,data_resource,university) VALUES ('การพัฒนาระบบการประสานรายการยาเดิมของผู้ป่วยในขั้นตอนแรกรับการรักษา โรงพยาบาลสงขลานครินทร์','เครือมาส แก้วทอน',2020,'OpenAlex','วลัยลักษณ์');

        """,
    )

    select_task_1 = SQLExecuteQueryOperator(
            task_id="select_task_1",
            conn_id="pg-1f2e1a3e",  # Replace with your Airflow connection ID
            sql="SELECT * FROM public.research order by years desc;", # Replace with your SELECT query
    )

    select_task_2 = SQLExecuteQueryOperator(
            task_id="select_task_2",
            conn_id="pg-1f2e1a3e",  # Replace with your Airflow connection ID
            sql="SELECT author,count(*) as n FROM public.research group by author order by n desc limit 10;", # Replace with your SELECT query
    )

    select_task_3 = SQLExecuteQueryOperator(
            task_id="select_task_3",
            conn_id="pg-1f2e1a3e",  # Replace with your Airflow connection ID
            sql="SELECT * FROM public.research where data_resource = 'Scopus' and years < 2022;", # Replace with your SELECT query
    )

create_table_task >> insert_data_task >> select_task_1 >> select_task_2 >> select_task_3

