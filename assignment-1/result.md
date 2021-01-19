# result

### Task 1 : Top-10 Active Taxis

('11DC93DD66D8A9A3DD9223122CF99EFD', 352)
('EE06BD8A621CAC3B608ACFDF0585A76A', 348)
('6C1132EF70BC0A7DB02174592F9A64A1', 341)
('A10A65AFD9F401BF3BDB79C84D3549E7', 340)
('23DB792D3F7EBA03004E470B684F2738', 339)
('7DA8DF1E4414F81EBD3A0140073B2630', 337)
('0318F7BBB8FF48688698F04016E67F49', 335)
('B07944BF31699A169091D2B16597A4A9', 334)
('738A62EEE9EC371689751A864C5EF811', 333)
('7D93E7FC4A7E4615A34B8286D92FF57F', 333)

The results are pairs of (taxi, number of unique driver) with descending order  by the number of unique drivers. Tuple[0] is the medallion and tuple[1] is counting the unique hack_license after reduce by medallion. The first tuple of the results can be explained as the taxi ID 11DC93DD66D8A9A3DD9223122CF99EFD is the most active taxi with 352 drivers

### Task 2: Top-10 Best Drivers

 ('E35D5869A2E09FFD1DE28C70D1D2E385', 236.25)
('ECA1B12A5C8EA203D4FE74D11D0BF881', 147.09677419354838)
('47E338B3C082945EFF04DE6D65915ADE', 131.25)
('5616060FB8AE85D93F334E7267307664', 92.3076923076923)
('6734FA703F6633AB896EECBDFAD8953A', 90.0)
('D67D8AB4F4C10BF22AA353E27879133C', 70.0)
('197636D970408B80F9A7736769BF548A', 52.5)
('15D8CAECB77542F0F9EE79C4F59D7A3B', 45.0)
('E4F99C9ABE9861F18BCD38BC63D007A9', 29.973130223134536)
('EDB446B67D69ADBFE9A21068982000C2', 20.0)

The results are pairs of (driver, average earned money per minute) with descending order by average earned money per minute. Tuple[0] is the hack_licence and tuple[1] is sum of total_amount/ sum of trip_time_in_secs  after reduce by hack_licence. The first tuple of the results can be explained as the driver ID E35D5869A2E09FFD1DE28C70D1D2E385 is the best driver with $236.25/min income. (Here, data with 0 trip_distance or 0 trip_time_in_secs have been considered as missing or imcompeleted data and have been removed from the dataset).

### Task3: Best time of the day to work on taxi

(17, 0.13833407623208557)
(18, 0.1355594548722469)
(19, 0.11014651225830889)
(21, 0.05910601736519538)
(2, 0.05798844308662319)
(16, 0.0576549999633559)
(22, 0.052019492551755225)
(20, 0.050747057423769594)
(0, 0.04729607809709379)
(23, 0.04611978964917874)
(4, 0.043550049348438355)
(1, 0.03693363940712342)
(3, 0.036182451511930065)
(5, 0.030866937904435853)
(15, 0.0015867212892034098)
(6, 0.00017377201085252316)
(10, 0.00012171915451295598)
(7, 0.00011966378341348402)
(13, 8.725483189085804e-05)
(11, 8.282385590884391e-05)
(12, 8.186309604244009e-05)
(9, 5.8945224127510875e-05)
(14, 4.688806476187454e-05)
(8, 4.2052496195290175e-05)

The results are pairs of (hour of the day, surcharge amount per mile) with descending order by surcharge amount per mile. Tuple[0] is the hour of the day and tuple[1] is sum of surcharge amount/ sum of trip_distance  after reduce by hour of the day. The first tuple of the results can be explained as the hour of the day 17 is the best time of the day to work on taxi with 0.13833407623208557 surcharge amount per mile. (Here, data with 0 trip_distance or 0 trip_time_in_secs have been considered as missing or imcompeleted data and have been removed from the dataset).
