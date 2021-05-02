# Pyspark-tutorial
******************************************************************************************************************************************************************************************************************************************************************************************************************************************************************
# I. Pyspark-RDD
## RDD (Tập dữ liệu phân tán có khả năng phục hồi) là gì?

RDD (Tập dữ liệu phân tán có khả năng phục hồi) là một khối xây dựng cơ bản của PySpark, là bộ sưu tập đối tượng phân tán không thay đổi, chịu được lỗi. Ý nghĩa bất biến khi bạn tạo RDD, bạn không thể thay đổi nó. Mỗi bản ghi trong RDD được chia thành các phân vùng logic, có thể được tính toán trên các nút khác nhau của cụm. 

Nói cách khác, RDD là một tập hợp các đối tượng tương tự như danh sách trong Python, với sự khác biệt là RDD được tính toán trên một số quy trình nằm rải rác trên nhiều máy chủ vật lý còn được gọi là các nút trong một cụm trong khi tập hợp Python tồn tại và xử lý chỉ trong một quy trình.

Ngoài ra, RDD cung cấp sự trừu tượng hóa dữ liệu của việc phân vùng và phân phối dữ liệu được thiết kế để chạy tính toán song song trên một số nút, trong khi thực hiện các phép biến đổi trên RDD, chúng ta không phải lo lắng về tính song song như PySpark cung cấp theo mặc định.

Hướng dẫn Apache PySpark RDD này mô tả các thao tác cơ bản có sẵn trên RDDs, chẳng hạn như  map(), filter()và  persist()và nhiều hơn nữa. Ngoài ra, hướng dẫn này cũng giải thích các hàm RDD Ghép nối hoạt động trên RDD của các cặp khóa-giá trị như  groupByKey() và join()v.v.
Lưu ý: RDD có thể có tên và số nhận dạng duy nhất (id)

## Lợi ích của PySpark RDD

PySpark được thích nghi rộng rãi trong cộng đồng Học máy và Khoa học dữ liệu do những ưu điểm của nó so với lập trình python truyền thống.

##Xử lý trong bộ nhớ

PySpark tải dữ liệu từ đĩa và xử lý trong bộ nhớ và giữ dữ liệu trong bộ nhớ, đây là điểm khác biệt chính giữa PySpark và Mapreduce (I/O chuyên sâu). Giữa các lần biến đổi, chúng ta cũng có thể lưu cache / duy trì RDD trong bộ nhớ để sử dụng lại các tính toán trước đó.

## Tạo RDD

RDD được tạo ra chủ yếu theo hai cách khác nhau,song song hóa một bộ sưu tập hiện có vàtham khảo một tập dữ liệu trong một hệ thống bên ngoài lưu trữ ( HDFS, S3và nhiều hơn nữa). 
Trước khi chúng ta xem xét các ví dụ, trước tiên hãy khởi tạo SparkSession bằng cách sử dụng phương thức mẫu xây dựng được định nghĩa trong lớp SparkSession. Trong khi khởi tạo, chúng ta cần cung cấp tên chính và ứng dụng như hình bên dưới. Trong ứng dụng thời gian thực, bạn sẽ vượt qua master từ spark-submit thay vì hardcoding trên ứng dụng Spark

![image](https://user-images.githubusercontent.com/64195026/109417729-7151c780-79f7-11eb-9037-862adddd2d75.png)

master() - Nếu bạn đang chạy nó trên cụm, bạn cần sử dụng tên chính của mình làm đối số cho chủ (). thông thường, nó sẽ là một trong hai  yarn (Yet Another Resource Negotiator)hoặc  mesos tùy thuộc vào thiết lập cụm của bạn.

Sử dụng local[x]khi chạy ở chế độ Độc lập. x phải là một giá trị nguyên và phải lớn hơn 0; điều này thể hiện số lượng phân vùng nó sẽ tạo khi sử dụng RDD, DataFrame và Dataset. Tốt nhất, giá trị x phải là số lõi CPU bạn có.

appName() - Được sử dụng để đặt tên ứng dụng của bạn.

getOrCreate() - Điều này trả về một đối tượng SparkSession nếu đã tồn tại, tạo một đối tượng mới nếu chưa tồn tại.

### Tạo RDD bằng sparkContext.parallelize ()
Bằng cách sử dụng parallelize()hàm của SparkContext ( sparkContext.parallelize () ), bạn có thể tạo RDD. Hàm này tải bộ sưu tập hiện có từ chương trình trình điều khiển của bạn vào song song hóa RDD. Đây là phương pháp cơ bản để tạo RDD và được sử dụng khi bạn đã có dữ liệu trong bộ nhớ được tải từ tệp hoặc từ cơ sở dữ liệu. và nó yêu cầu tất cả dữ liệu phải có trên chương trình trình điều khiển trước khi tạo RDD.

![image](https://user-images.githubusercontent.com/64195026/109417742-77e03f00-79f7-11eb-9e33-cba5b84f9f93.png)

### Tạo RDD bằng sparkContext.textFile ()

Sử dụng phương thức textFile (), chúng ta có thể đọc tệp văn bản (.txt) vào RDD.

![image](https://user-images.githubusercontent.com/64195026/109417744-7adb2f80-79f7-11eb-9142-b5ae0aeb8d97.png)

### Tạo RDD bằng sparkContext.wholeTextFiles ()

Hàm wholeTextFiles () trả về một PairRDD với khóa là đường dẫn tệp và giá trị là nội dung tệp.

![image](https://user-images.githubusercontent.com/64195026/109417748-7d3d8980-79f7-11eb-827f-1a98bf186292.png)

Bên cạnh việc sử dụng các tệp văn bản, chúng ta cũng có thể tạo RDD từ tệp CSV , JSON và nhiều định dạng khác.

### Tạo RDD trống bằng sparkContext.emptyRDD

Sử dụng emptyRDD()phương thức trên sparkContext, chúng ta có thể  tạo một RDD không có dữ liệu . Phương pháp này tạo ra một RDD trống không có phân vùng

![image](https://user-images.githubusercontent.com/64195026/109417754-80387a00-79f7-11eb-9aca-cfae13aa39b2.png)

### Tạo RDD trống với phân vùng

Đôi khi, chúng ta có thể cần ghi RDD trống vào các tệp theo phân vùng, Trong trường hợp này, bạn nên tạo RDD trống có phân vùng.

![image](https://user-images.githubusercontent.com/64195026/109417759-862e5b00-79f7-11eb-8045-eb2eba31b8cc.png)

### RDD Song song hóa

Khi chúng tôi sử dụng parallelize()hoặc textFile()hoặc  wholeTextFiles()các phương thức của SparkContxt để khởi tạo RDD, nó sẽ tự động chia dữ liệu thành các phân vùng dựa trên tính khả dụng của tài nguyên. khi bạn chạy nó trên máy tính xách tay, nó sẽ tạo các phân vùng có cùng số lượng lõi có sẵn trên hệ thống của bạn.

getNumPartitions () - Đây là một hàm RDD trả về một số phân vùng mà tập dữ liệu của chúng tôi được chia thành.

![image](https://user-images.githubusercontent.com/64195026/109417927-4451e480-79f8-11eb-9d43-bb99fc44eb9c.png)

Đặt song song theo cách thủ công - Chúng ta cũng có thể đặt một số phân vùng theo cách thủ công, tất cả những gì chúng ta cần là chuyển một số phân vùng làm tham số thứ hai cho các hàm này chẳng hạn   sparkContext.parallelize([1,2,3,4,56,7,8,9,12,3], 10).

# Pyspark-properties

Thuộc tính Spark kiểm soát hầu hết các cài đặt ứng dụng và được cấu hình riêng cho từng ứng dụng. Các thuộc tính này có thể được đặt trực tiếp trên SparkConf được chuyển đến của bạn SparkContext. SparkConfcho phép bạn định cấu hình một số thuộc tính chung (ví dụ: URL chính và tên ứng dụng), cũng như các cặp khóa-giá trị tùy ý thông qua set()phương thức. Ví dụ, chúng ta có thể khởi tạo một ứng dụng với hai luồng như sau:
Lưu ý rằng chúng tôi chạy với local [2], nghĩa là hai luồng - thể hiện sự song song “tối thiểu”, có thể giúp phát hiện lỗi chỉ tồn tại khi chúng tôi chạy trong bối cảnh phân tán.

![1](https://user-images.githubusercontent.com/64195026/109417294-8d546980-79f5-11eb-9d10-bb8e449fef2b.png)

Lưu ý rằng chúng ta có thể có nhiều hơn 1 luồng ở chế độ cục bộ và trong những trường hợp như Spark Streaming, chúng tôi thực sự có thể yêu cầu nhiều hơn 1 luồng để ngăn chặn bất kỳ loại vấn đề chết đói nào.
Các thuộc tính chỉ định một số khoảng thời gian nên được cấu hình với một đơn vị thời gian. Định dạng sau được chấp nhận:

![2](https://user-images.githubusercontent.com/64195026/109417503-8548f980-79f6-11eb-86f1-9bf8e3490e4f.png)

Thuộc tính chỉ định kích thước byte phải được cấu hình với đơn vị kích thước. Định dạng sau được chấp nhận:

![3](https://user-images.githubusercontent.com/64195026/109417506-867a2680-79f6-11eb-95d2-da179c415dd2.png)

Trong khi các số không có đơn vị thường được hiểu là byte, một số ít được hiểu là KiB hoặc MiB. Xem tài liệu về các thuộc tính cấu hình riêng lẻ. Việc chỉ định đơn vị là mong muốn nếu có thể.

## Thuộc tính có sẵn
Hầu hết các thuộc tính kiểm soát cài đặt nội bộ đều có giá trị mặc định hợp lý. Một số tùy chọn phổ biến nhất để đặt là:

## Thuộc tính ứng dụng
![4](https://user-images.githubusercontent.com/64195026/109417457-57fc4b80-79f6-11eb-8e93-c1513a0454da.png)
![5](https://user-images.githubusercontent.com/64195026/109417463-5a5ea580-79f6-11eb-995f-399050536fc3.png)
![6](https://user-images.githubusercontent.com/64195026/109417465-5c286900-79f6-11eb-8877-1cd7f286f742.png)
![7](https://user-images.githubusercontent.com/64195026/109417468-5e8ac300-79f6-11eb-89ae-2710f8da9109.png)
![8](https://user-images.githubusercontent.com/64195026/109417469-5fbbf000-79f6-11eb-9c97-bd0977ed1e37.png)
![9](https://user-images.githubusercontent.com/64195026/109417471-60ed1d00-79f6-11eb-9537-96ce6bcbb80f.png)
![10](https://user-images.githubusercontent.com/64195026/109417475-62b6e080-79f6-11eb-8468-4ab8bdaa3756.png)
![11](https://user-images.githubusercontent.com/64195026/109417477-63e80d80-79f6-11eb-98c6-15dc96d0d6c3.png)
![12](https://user-images.githubusercontent.com/64195026/109417480-65b1d100-79f6-11eb-8e16-bd499226880f.png)

##Môi trường thực thi

![13](https://user-images.githubusercontent.com/64195026/109417518-9134bb80-79f6-11eb-9317-2e3cf9fc1c1a.png)
![14](https://user-images.githubusercontent.com/64195026/109417519-9265e880-79f6-11eb-9e2a-f493a30b980a.png)
![15](https://user-images.githubusercontent.com/64195026/109417520-93971580-79f6-11eb-9d8d-ffd7b0d1b034.png)
![16](https://user-images.githubusercontent.com/64195026/109417523-94c84280-79f6-11eb-8baa-7220abc07871.png)
![17](https://user-images.githubusercontent.com/64195026/109417524-96920600-79f6-11eb-921a-c4592cca74b4.png)
![18](https://user-images.githubusercontent.com/64195026/109417525-97c33300-79f6-11eb-9aad-bfbf9914d78c.png)


# Pyspark-dataframe

## PySpark - Tạo DataFrame với các ví dụ

Bạn có thể Tạo một PySpark DataFrame bằng cách sử dụng toDF()và createDataFrame()các phương pháp, cả hai hàm này đều lấy các chữ ký khác nhau để tạo DataFrame từ RDD, danh sách và DataFrame hiện có.

Bạn cũng có thể tạo PySpark DataFrame từ các nguồn dữ liệu như TXT, CSV, JSON, ORV, Avro, Parquet, định dạng XML bằng cách đọc từ hệ thống tệp HDFS, S3, DBFS, Azure Blob, v.v.
Cuối cùng, PySpark DataFrame cũng có thể được tạo bằng cách đọc dữ liệu từ Cơ sở dữ liệu RDBMS và Cơ sở dữ liệu NoSQL.

## Tạo DataFrame từ RDD

Một cách dễ dàng để tạo PySpark DataFrame là từ một RDD hiện có. đầu tiên, hãy tạo một Spark RDD từ một Danh sách bộ sưu tập bằng cách gọi hàm song song () từ SparkContext . Chúng tôi sẽ cần đối tượng rdd này cho tất cả các ví dụ của chúng tôi bên dưới.

![image](https://user-images.githubusercontent.com/64195026/109418018-af9bb680-79f8-11eb-9d4d-3c192cf5af63.png)

### Sử dụng hàm toDF ()

Phương thức toDF () của PySpark RDD được sử dụng để tạo DataFrame từ RDD hiện có. Vì RDD không có 

![image](https://user-images.githubusercontent.com/64195026/109418046-de199180-79f8-11eb-9fb7-dd9fcf221398.png)

Nếu bạn muốn cung cấp tên cột cho toDF() phương pháp sử dụng DataFrame với tên cột làm đối số như hình dưới đây.

![image](https://user-images.githubusercontent.com/64195026/109418058-f2f62500-79f8-11eb-86d4-497b8050b596.png)

Điều này tạo ra lược đồ của DataFrame với các tên cột.

![image](https://user-images.githubusercontent.com/64195026/109418065-fc7f8d00-79f8-11eb-9d1f-9b802e50f823.png)

Theo mặc định, kiểu dữ liệu của các cột này suy ra kiểu dữ liệu. Chúng ta có thể thay đổi hành vi này bằng cách cung cấp lược đồ , trong đó chúng ta có thể chỉ định tên cột, kiểu dữ liệu và giá trị có thể làm trống cho mỗi trường / cột.

### Sử dụng createDataFrame () từ SparkSession

Sử dụng createDataFrame () từ SparkSession là một cách khác để tạo và nó lấy đối tượng rdd làm đối số. và chuỗi với toDF () để chỉ định tên cho các cột.

![image](https://user-images.githubusercontent.com/64195026/109418086-13be7a80-79f9-11eb-811d-41b2039cabfd.png)

## Tạo DataFrame từ Bộ sưu tập danh sách

Trong phần này, chúng ta sẽ xem cách tạo PySpark DataFrame từ một danh sách. Những ví dụ này sẽ tương tự như những gì chúng ta đã thấy trong phần trên với RDD, nhưng chúng ta sử dụng đối tượng dữ liệu danh sách thay vì đối tượng “rdd” để tạo DataFrame.

### Sử dụng createDataFrame () từ SparkSession

Gọi createDataFrame()from SparkSession là một cách khác để tạo PySpark DataFrame, nó lấy một đối tượng danh sách làm đối số. và chuỗi với toDF()để chỉ định tên cho các cột.

![image](https://user-images.githubusercontent.com/64195026/109418126-4a949080-79f9-11eb-918b-28663713c49d.png)

### Sử dụng createDataFrame () với kiểu Hàng

createDataFrame()có một chữ ký khác trong PySpark lấy bộ sưu tập kiểu Hàng và lược đồ cho tên cột làm đối số. Để sử dụng điều này, trước tiên chúng ta cần chuyển đổi đối tượng “dữ liệu” từ danh sách sang danh sách Hàng.

![image](https://user-images.githubusercontent.com/64195026/109418134-584a1600-79f9-11eb-940a-7162fa2b8bc0.png)

### Tạo DataFrame bằng lược đồ

Nếu bạn muốn chỉ định tên cột cùng với kiểu dữ liệu của chúng, trước tiên bạn nên tạo lược đồ StructType và sau đó gán nó trong khi tạo DataFrame.

![image](https://user-images.githubusercontent.com/64195026/109418154-69932280-79f9-11eb-9563-57c973dd7b13.png)

Điều này dẫn đến sản lượng thấp hơn.

![image](https://user-images.githubusercontent.com/64195026/109418163-731c8a80-79f9-11eb-8e93-a210920e2c05.png)

##  Tạo DataFrame từ các nguồn Dữ liệu

Trong thời gian thực, hầu hết bạn tạo DataFrame từ các tệp nguồn dữ liệu như CSV, Văn bản, JSON, XML, v.v.
PySpark theo mặc định hỗ trợ nhiều định dạng dữ liệu mà không cần nhập bất kỳ thư viện nào và để tạo DataFrame, bạn cần sử dụng phương pháp thích hợp có sẵn trong DataFrameReaderlớp.

### Tạo DataFrame từ CSV

Sử dụng csv()phương thức của DataFrameReaderđối tượng để tạo DataFrame từ tệp CSV. bạn cũng có thể cung cấp các tùy chọn như dấu phân cách sẽ sử dụng, cho dù bạn đã trích dẫn dữ liệu, định dạng ngày tháng, lược đồ suy luận, v.v. Vui lòng tham khảo PySpark Read CSV thành DataFrame
 
 ![image](https://user-images.githubusercontent.com/64195026/109418193-a4955600-79f9-11eb-92c5-62b0a8590e24.png)

 
### Tạo từ tệp văn bản (TXT)

Tương tự, bạn cũng có thể tạo DataFrame bằng cách đọc từ tệp Văn bản, sử dụng text()phương thức của DataFrameReader để làm như vậy.

![image](https://user-images.githubusercontent.com/64195026/109418200-ac54fa80-79f9-11eb-80e4-a85bd8a9a4d9.png)


### Tạo từ tệp JSON

PySpark cũng được sử dụng để xử lý các tệp dữ liệu bán cấu trúc như định dạng JSON. bạn có thể sử dụng json()phương thức của DataFrameReader để đọc tệp JSON vào DataFrame. Dưới đây là một ví dụ đơn giản.

![image](https://user-images.githubusercontent.com/64195026/109418215-b676f900-79f9-11eb-89fc-bfa87a46f4fc.png)


 Tương tự, chúng ta có thể tạo DataFrame trong PySpark từ hầu hết các cơ sở dữ liệu quan hệ mà tôi chưa trình bày ở đây và tôi sẽ để bạn khám phá.
 
## Các nguồn khác (Avro, Parquet, ORC, Kafka)

Có thể tạo DataFrame bằng cách đọc các tệp Avro, Parquet, ORC, Binary và truy cập bảng Hive và HBase, đồng thời đọc dữ liệu từ Kafka mà tôi đã giải thích trong các bài viết dưới đây, tôi khuyên bạn nên đọc chúng khi có thời gian.


# Hướng dẫn Apache Spark: ML với PySpark

Apache Spark được biết đến như một công cụ nhanh, dễ sử dụng và chung để xử lý dữ liệu lớn có các mô-đun tích hợp để xử lý luồng, SQL, Machine Learning (ML) và xử lý đồ thị. Công nghệ này là một kỹ năng cần thiết cho các kỹ sư dữ liệu, nhưng các nhà khoa học dữ liệu cũng có thể hưởng lợi từ việc học Spark khi thực hiện Phân tích dữ liệu khám phá (EDA), trích xuất tính năng và tất nhiên là ML.

## Các bước để xây dựng một chương trình Học máy với PySpark:

+ Bước 1 Hoạt động cơ bản với PySpark
+ Bước 2) Tiền xử lý dữ liệu
+ Bước 3) Xây dựng pipeline xử lý dữ liệu
+ Bước 4) Xây dựng bộ phân loại: logistic
+ Bước 5) Đào tạo và đánh giá mô hình
+ Bước 6) Điều chỉnh siêu tham số
chúng ta sẽ sử dụng tập dữ liệu adult dataset. Mục đích của hướng dẫn này là để học cách sử dụng Pyspark.

### Bước 1 Hoạt động cơ bản với PySpark

Trước hết, bạn cần khởi tạo SQLContext.

![image](https://user-images.githubusercontent.com/64195026/116817432-803c2e00-ab90-11eb-9109-7fd684fd1245.png)

Sau đó, bạn có thể đọc tệp cvs bằng sqlContext.read.csv. Sử dụng  inferSchema được đặt thành True để yêu cầu Spark tự động đoán loại dữ liệu. Theo mặc định, nó chuyển thành False.

![image](https://user-images.githubusercontent.com/64195026/116817447-90540d80-ab90-11eb-967f-a533a5b0a7cf.png)

Hãy xem kiểu dữ liệu

![image](https://user-images.githubusercontent.com/64195026/116817458-9fd35680-ab90-11eb-88b0-026a3337d02b.png)

Bạn có thể xem dữ liệu vs shaow

![image](https://user-images.githubusercontent.com/64195026/116817476-be395200-ab90-11eb-9631-52152973e8b2.png)

Nếu bạn không đặt inderShema thành True, đây là những gì đang xảy ra với type. Có tất cả trong chuỗi.

![image](https://user-images.githubusercontent.com/64195026/116817521-f3de3b00-ab90-11eb-8b9c-c5b2569fad7b.png)

Để chuyển đổi biến liên tục theo đúng định dạng, bạn có thể sử dụng các cột. Bạn có thể sử dụng withColumn để cho Spark biết cột nào sẽ hoạt động chuyển đổi.

![image](https://user-images.githubusercontent.com/64195026/116817556-14a69080-ab91-11eb-93fc-54c01444507d.png)

Select columns Bạn có thể chọn và hiển thị các hàng có lựa chọn và tên của các đặc trưng. Dưới đây, age và fnlwgt được chọn.

![image](https://user-images.githubusercontent.com/64195026/116817573-2ee06e80-ab91-11eb-8b7b-3e755a282a05.png)

Count by group Nếu bạn muốn đếm số lần xuất hiện theo nhóm, bạn có thể xâu chuỗi:
+ groupBy()
+ count()

Trong ví dụ PySpark bên dưới, bạn đếm số hàng theo education level.

![image](https://user-images.githubusercontent.com/64195026/116817587-4c153d00-ab91-11eb-8b30-e9eba71e1718.png)

Describe the data Để nhận thống kê tóm tắt về dữ liệu, bạn có thể sử dụng description():
+ count
+ mean
+ standarddeviation
+ min
+ max

![image](https://user-images.githubusercontent.com/64195026/116817611-6bac6580-ab91-11eb-80a2-223ef01cfcaf.png)

Nếu bạn muốn thống kê tóm tắt chỉ của một cột, hãy thêm tên của cột vào bên trong description().

![image](https://user-images.githubusercontent.com/64195026/116817622-79fa8180-ab91-11eb-80ba-85f7870f68fb.png)

Crosstab computation Trong một số trường hợp, có thể thú vị khi xem các thống kê mô tả giữa hai cột theo cặp. Ví dụ: bạn có thể đếm số người có thu nhập dưới hoặc trên 50k theo trình độ học vấn. Thao tác này được gọi là crosstab.

![image](https://user-images.githubusercontent.com/64195026/116817634-91396f00-ab91-11eb-9f93-6059fb5b632a.png)

Drop column Có hai API trực quan để drop columns:
+ drop(): Drop a column
+ dropna(): Drop NA’s
Bên dưới bạn drop column  education_num

![image](https://user-images.githubusercontent.com/64195026/116817665-b3cb8800-ab91-11eb-8f94-728b02a2bf65.png)

Filter data Bạn có thể sử dụng filter () để áp dụng thống kê mô tả trong một tập hợp con dữ liệu. Ví dụ: bạn có thể đếm số người trên 40 tuổi

![image](https://user-images.githubusercontent.com/64195026/116817690-d52c7400-ab91-11eb-9e4d-52235c8d5ed5.png)

Thống kê mô tả theo nhóm Cuối cùng, bạn có thể nhóm dữ liệu theo nhóm và tính toán các hoạt động thống kê như giá trị trung bình.

![image](https://user-images.githubusercontent.com/64195026/116817701-e5445380-ab91-11eb-9133-7089bbacf1da.png)

## Bước 2 Tiền xử lý dữ liệu
Xử lý dữ liệu là một bước quan trọng trong học máy. Sau khi xóa dữ liệu rác, bạn sẽ có được một số thông tin chi tiết quan trọng.

Ví dụ, bạn biết rằng tuổi không phải là một hàm tuyến tính với thu nhập. Khi còn trẻ, thu nhập của họ thường thấp hơn tuổi trung niên. Sau khi nghỉ hưu, một hộ gia đình sử dụng tiền tiết kiệm của họ, nghĩa là thu nhập giảm. Để chụp mẫu này, bạn có thể thêm square vào đặc trưng tuổi.

Add age square Để thêm một đặc trưng mới, bạn cần:
+ Chọn cột
+ Áp dụng phép biến đổi và thêm nó vào DataFrame

![image](https://user-images.githubusercontent.com/64195026/116817736-158bf200-ab92-11eb-9e45-88bd59126069.png)

Bạn có thể thấy rằng age_square đã được thêm thành công vào khung dữ liệu. Bạn có thể thay đổi thứ tự của các biến với select. Dưới đây, bạn mang theo age_square ngay sau tuổi.

![image](https://user-images.githubusercontent.com/64195026/116817749-22a8e100-ab92-11eb-85fa-8f79798575c1.png)

Loại trừ Holand-Netherlands
Khi một nhóm trong một đặc trưng chỉ có một dữ liệu, nó không mang lại thông tin gì cho mô hình. Ngược lại, nó có thể dẫn đến lỗi trong quá trình cross-validation.

Hãy kiểm tra nguồn gốc của hộ.

![image](https://user-images.githubusercontent.com/64195026/116817766-35bbb100-ab92-11eb-8b35-edb3bd769acd.png)

Đặc trưng native_country chỉ có một hộ gia đình đến từ Hà Lan. Bạn loại trừ nó.

![image](https://user-images.githubusercontent.com/64195026/116817780-4704bd80-ab92-11eb-9a98-5b1da66f69ca.png)

## Bước 3 Xây dựng pipeline xử lý dữ liệu
Tương tự như scikit-learn, Pyspark có API pipeline.

Một pipeline dẫn rất thuận tiện để duy trì cấu trúc của dữ liệu. Bạn đẩy dữ liệu vào pipeline. Bên trong pipeline, các hoạt động khác nhau được thực hiện, đầu ra được sử dụng để cung cấp cho thuật toán.

Ví dụ: một phép biến đổi phổ quát trong học máy bao gồm chuyển đổi một chuỗi thành một one hot encoder, tức là một cột theo nhóm. One hot encoder thường là một ma trận đầy các số 0.

Các bước để biến đổi dữ liệu rất giống với scikit-learn. Bạn cần phải:

+ Lập index chuỗi thành số
+ Tạo một bộ one hot encoder
+ Chuyển đổi dữ liệu
+ Hai API thực hiện công việc: StringIndexer, OneHotEncoder

1. Trước hết, bạn chọn cột chuỗi để lập chỉ mục. InputCol là tên của cột trong tập dữ liệu. OutputCol là tên mới được đặt cho cột được chuyển đổi.
StringIndexer(inputCol="workclass", outputCol="workclass_encoded")

![image](https://user-images.githubusercontent.com/64195026/116817807-6dc2f400-ab92-11eb-949d-4eeb702faaf6.png)

2. Điều chỉnh dữ liệu và biến đổi nó

![image](https://user-images.githubusercontent.com/64195026/116817829-8af7c280-ab92-11eb-9aa6-02e8a4687b5f.png)

3. Tạo các cột news dựa trên nhóm. Ví dụ: nếu có 10 nhóm trong đặc trưng, ma trận mới sẽ có 10 cột, mỗi nhóm một cột.

![image](https://user-images.githubusercontent.com/64195026/116817838-9519c100-ab92-11eb-89e4-c1012f426984.png)

![image](https://user-images.githubusercontent.com/64195026/116817850-a4990a00-ab92-11eb-8a84-53f1530ae68e.png)

![image](https://user-images.githubusercontent.com/64195026/116817858-acf14500-ab92-11eb-9bdd-ad99acab6e19.png)


Xây dựng pipeline Bạn sẽ xây dựng một pipeline để chuyển đổi tất cả các đặc trưng chính xác và thêm chúng vào tập dữ liệu cuối cùng. Pipeline sẽ có bốn hoạt động, nhưng hãy thoải mái thêm bao nhiêu hoạt động tùy thích.
1. Encode dữ liệu phân loại
2. Lập Index label feature
3. Thêm biến liên tục
4. Tập hợp các bước.

Mỗi bước được lưu trữ trong một danh sách có tên các giai đoạn. Danh sách này sẽ cho VectorAssembler biết thao tác nào cần thực hiện bên trong pipeline.

Mã hóa dữ liệu phân loại Bước này cũng giống như ví dụ trên, ngoại trừ việc bạn lặp lại tất cả các đặc trưng phân loại.

![image](https://user-images.githubusercontent.com/64195026/116817902-e0cc6a80-ab92-11eb-9b26-7da4cfcc7c6a.png)

Lập index label feature Spark, giống như nhiều thư viện khác, không chấp nhận các giá trị chuỗi cho nhãn. Bạn chuyển đổi đặc trưng nhãn với StringIndexer và thêm nó vào các giai đoạn danh sách.

![image](https://user-images.githubusercontent.com/64195026/116817937-f9d51b80-ab92-11eb-807d-80764a732424.png)

Thêm biến liên tục InputCols của VectorAssembler là một danh sách các cột. Bạn có thể tạo một danh sách mới chứa tất cả các cột mới.

![image](https://user-images.githubusercontent.com/64195026/116817945-048fb080-ab93-11eb-822b-462f36826e36.png)

Tập hợp các bước Cuối cùng, bạn vượt qua tất cả các bước trong VectorAssembler

![image](https://user-images.githubusercontent.com/64195026/116817962-12ddcc80-ab93-11eb-8f18-8b2de89a0454.png)

Bây giờ tất cả các bước đã sẵn sàng, bạn đẩy dữ liệu vào pipeline.

![image](https://user-images.githubusercontent.com/64195026/116817970-1c673480-ab93-11eb-9d0e-bd0f1c4105b2.png)

Nếu bạn kiểm tra tập dữ liệu mới, bạn có thể thấy rằng nó chứa tất cả các đặc trưng, được chuyển đổi và chưa được chuyển đổi. Bạn chỉ quan tâm đến nhãn mới và các đặc trưng.

![image](https://user-images.githubusercontent.com/64195026/116817986-3739a900-ab93-11eb-873b-f853945b75c7.png)

## Bước 4 Xây dựng bộ phân loại: logistic

Để tính toán nhanh hơn, bạn chuyển đổi mô hình thành DataFrame.
Bạn cần chọn nhãn mới và các đặc trưng từ mô hình bằng cách sử dụng map.

![image](https://user-images.githubusercontent.com/64195026/116818000-50425a00-ab93-11eb-84e3-4f4df20b8716.png)

Bạn đã sẵn sàng tạo dữ liệu train dưới dạng DataFrame. Sử dụng sqlContext

![image](https://user-images.githubusercontent.com/64195026/116818012-5df7df80-ab93-11eb-9e03-ea97561da5a2.png)

Kiểm tra hàng thứ hai

![image](https://user-images.githubusercontent.com/64195026/116818027-7405a000-ab93-11eb-93d8-e8c120b0be0f.png)

Tạo train/test set Bạn chia tập dữ liệu 80/20 với randomSplit.

![image](https://user-images.githubusercontent.com/64195026/116818049-94355f00-ab93-11eb-9c50-33fae03abff2.png)

Hãy đếm xem có bao nhiêu người có thu nhập dưới / trên 50k trong cả tập huấn luyện và kiểm tra.

![image](https://user-images.githubusercontent.com/64195026/116818071-af07d380-ab93-11eb-8fed-366c0e2ab3c5.png)

Xây dựng bộ hồi quy logistic
Cuối cùng nhưng không kém phần quan trọng, bạn có thể xây dựng bộ phân loại. Pyspark có một API gọi là LogisticRegression để thực hiện hồi quy logistic.

Bạn khởi tạo lr bằng cách chỉ ra cột nhãn và các cột đặc trưng. Đặt tối đa 10 lần lặp và thêm thông số chính quy hóa với giá trị 0,3. Lưu ý rằng trong phần tiếp theo, bạn sẽ sử dụng xác thực chéo với lưới tham số để điều chỉnh mô hình.

![image](https://user-images.githubusercontent.com/64195026/116818090-be871c80-ab93-11eb-899d-f7da3e698c9d.png)

# Bạn có thể xem các hệ số từ hồi quy

![image](https://user-images.githubusercontent.com/64195026/116818103-c9da4800-ab93-11eb-85b3-1ddb7d521247.png)

## Bước 5 Đào tạo và đánh giá mô hình
Để tạo dự đoán cho bộ thử Bạn cần phải xem chỉ số độ chính xác để xem mô hình hoạt động tốt (hoặc xấu) như thế nào.nghiệm của bạn. Bạn có thể sử dụng linearModel với transform() trên test_data.

![image](https://user-images.githubusercontent.com/64195026/116818119-e0809f00-ab93-11eb-928b-f32bc1769cbb.png)
Bạn có thể in các phần tử trong dự đoán

![image](https://user-images.githubusercontent.com/64195026/116818122-e6768000-ab93-11eb-9764-905ab415be1c.png)

Bạn quan tâm đến nhãn, dự đoán và xác suất

![image](https://user-images.githubusercontent.com/64195026/116818140-f2fad880-ab93-11eb-86c5-b44295b6f944.png)

Đánh giá mô hình Bạn cần phải xem chỉ số độ chính xác để xem mô hình hoạt động tốt (hoặc xấu) như thế nào. Hiện tại, không có API nào để tính toán độ chính xác trong Spark. Giá trị mặc định là ROC (receiver operating characteristic curve).

Trước khi bạn xem xét ROC, hãy xây dựng thước đo độ chính xác. Thước đo độ chính xác là tổng của dự đoán đúng trên tổng số quan sát.

Bạn tạo một DataFrame với nhãn và dự đoán

![image](https://user-images.githubusercontent.com/64195026/116818163-19207880-ab94-11eb-97ef-2b5924a55112.png)

Bạn có thể kiểm tra số lượng lớp trong nhãn và dự đoán

![image](https://user-images.githubusercontent.com/64195026/116818169-250c3a80-ab94-11eb-82af-347ad962383b.png)

Ví dụ, trong tập thử nghiệm, có 1578 hộ gia đình có thu nhập trên 50k và 5021 hộ dưới. Tuy nhiên, phân loại dự đoán 617 hộ gia đình có thu nhập trên 50 nghìn.

Bạn có thể tính độ chính xác bằng cách tính số lượng khi nhãn được phân loại chính xác trên tổng số hàng.

![image](https://user-images.githubusercontent.com/64195026/116818180-348b8380-ab94-11eb-8d65-2678caf96364.png)

Bạn có thể kết hợp mọi thứ lại với nhau và viết một hàm để tính độ chính xác.

![image](https://user-images.githubusercontent.com/64195026/116818185-3fdeaf00-ab94-11eb-8a09-76b96a6f2a5c.png)

ROC metrics
Mô-đun BinaryClassificationEvaluator bao gồm các biện pháp ROC. Receiver Operating Characteristic curve là một công cụ phổ biến khác được sử dụng với phân loại nhị phân. Nó rất giống với precision/recall nhưng thay vì vẽ biểu đồ precision so với recall. ROC cho thấy tỷ lệ dương tính thực sự (tức là recall) so với tỷ lệ dương tính giả.Tỷ lệ dương tính giả là tỷ lệ các trường hợp tiêu cực được phân loại không chính xác là dương tính. Tỷ lệ âm thực sự còn được gọi là độ đặc hiệu. Do đó, đường cong ROC biểu thị độ nhạy (recall) so với 1 – độ đặc hiệu.

![image](https://user-images.githubusercontent.com/64195026/116818190-4c630780-ab94-11eb-9623-5d414d9dbd89.png)
![image](https://user-images.githubusercontent.com/64195026/116818207-67357c00-ab94-11eb-81d0-f7ef00817f55.png)

## Bước 6 Điều chỉnh siêu tham số

Cuối cùng nhưng không kém phần quan trọng, bạn có thể điều chỉnh các siêu tham số.
Để giảm thời gian tính toán, bạn chỉ điều chỉnh tham số chính quy chỉ với hai giá trị.

![image](https://user-images.githubusercontent.com/64195026/116818222-7b797900-ab94-11eb-8783-f3139bdaf8b4.png)

Cuối cùng, bạn đánh giá mô hình bằng cách sử dụng phương pháp cross valiation.

![image](https://user-images.githubusercontent.com/64195026/116818231-86340e00-ab94-11eb-9740-5773a1f4c5c9.png)

Thời gian đào tạo mô hình: 978.807 giây
Siêu tham số đo chính quy tốt nhất là 0,01, với độ chính xác 85,316 phần trăm.

![image](https://user-images.githubusercontent.com/64195026/116818246-96e48400-ab94-11eb-8665-24bbca7b8dc5.png)

Bạn có thể loại trừ tham số được đề xuất bằng cách chaining cvModel.bestModel với extractParamMap().

{Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='aggregationDepth', doc='suggested depth for treeAggregate (>= 2)'): 2,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='elasticNetParam', doc='the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty'): 0.0,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='family', doc='The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial.'): 'auto',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='featuresCol', doc='features column name'): 'features',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='fitIntercept', doc='whether to fit an intercept term'): True,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='labelCol', doc='label column name'): 'label',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='maxIter', doc='maximum number of iterations (>= 0)'): 10,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='predictionCol', doc='prediction column name'): 'prediction',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='probabilityCol', doc='Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities'): 'probability',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='rawPredictionCol', doc='raw prediction (a.k.a. confidence) column name'): 'rawPrediction',
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='regParam', doc='regularization parameter (>= 0)'): 0.01,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='standardization', doc='whether to standardize the training features before fitting the model'): True,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='threshold', doc='threshold in binary classification prediction, in range [0, 1]'): 0.5,
 Param(parent='LogisticRegression_4d8f8ce4d6a02d8c29a0', name='tol', doc='the convergence tolerance for iterative algorithms (>= 0)'): 1e-06}
