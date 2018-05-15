%% Map Reduce times per file size
%gb1 = [44.206261 24.344702 14.993695 13.794354 13.214474 20.539711 19.611258];
gb1 = [58.179223 38.344702 30.209526 27.794354 27.320309 34.539711 33.611258];
%           1      2         4           6         8                  16
%gb2 = [93.469804 51.395925 31.426143 29.283976 29.02236 37.409714 41.858639];
gb2 = [121.903450 81.395925 64.357541 55.283976 54.217165 61.409714 66.751160]

%gb2_8= [8234.86493 2106.017669 4120.272176 5987.08495];
gb2_8 = [12731.958691 6686.974704 8586.499522 10489.991414]
threadNum = [1 2 4 6 8 12 16];
threadNum1 = [1 4 8 16];
plot(threadNum, gb1, threadNum, gb2)
xlabel('Number of Threads')
ylabel('Time (s)')
legend('1 GB Input Files', '2 GB Input Files')

%% Map Reduce times per file size in a Log scale 
threadNum1 = [1 4 8 16];
loglog(threadNum, gb1, 'r')
hold on 
loglog(threadNum, gb2, 'k')
hold on 
loglog(threadNum1, gb2_8, 'b')
xlim([1 16])
ylim([10 100000])
xlabel('Number of Threads')
ylabel('Time (s)')
legend('1 GB Input Files', '2 GB Input Files', '2.8 GB Input Files')
set(gca,'XScale','linear','YScale','log')

% For this, once the graph has been made, change the x-axis to a linear
% scale
%% Map Reduce Speedup graphs

speedupFile1 = gb1(1) ./ gb1;
speedupFile2 = gb2(1) ./ gb2;
speedupFile3 = gb2_8(1) ./ gb2_8;

threadNum1 = [1 4 8 16];
plot(threadNum, speedupFile1, 'r')
hold on 
plot(threadNum, speedupFile2, 'k')
hold on 
plot(threadNum1, speedupFile3, 'b')
xlim([2 16])
xlabel('Number of Threads')
ylabel('Speedup (T_1/T_N)')
legend('1 GB Input Files', '2 GB Input Files', '2.8 GB Input Files')

%% Hybrid times per file size
gb1 = [44699.273660 21574.42871 13324.38398 9750.672016 6301.072246 4965.061558 4048.935617];
%           2      3         4           5         6           7        8
gb2 = [86398.547330 41869.46384 26134.32464 18629.13038 12230.7858 9458.883744 8114.742223];

gb2_8 = [120157.96630 58489.31001 36279.3885 24249.13389 16900.28489 13273.85308 10952.19782];
node_num = [2 3 4 5 6 7 8];
plot(node_num, gb1, node_num, gb2, node_num, gb2_8)
xlabel('Number of Nodes')
ylabel('Time (s)')
legend('1 GB Input Files', '2 GB Input Files', '2.8 GB Input Files')

%% Hybrid times per file size in a Log scale 
node_num1 = [2 3 4 5 6 7 8];
loglog(node_num, gb1, 'r')
hold on 
loglog(node_num, gb2, 'k')
hold on 
loglog(node_num1, gb2_8, 'b')
xlim([2 8])
xlabel('Number of Nodes')
ylabel('Time (s)')
legend('1 GB Input Files', '2 GB Input Files', '2.8 GB Input Files')
set(gca,'XScale','linear','YScale','log')

% For this, once the graph has been made, change the x-axis to a linear
% scale
%% Hybrid Speedup graphs

speedupFile1 = gb1(1) ./ gb1;
speedupFile2 = gb2(1) ./ gb2;
speedupFile3 = gb2_8(1) ./ gb2_8;

node_num1 = [2 3 4 5 6 7 8];
plot(node_num, speedupFile1, 'r')
hold on 
plot(node_num, speedupFile2, 'k')
hold on 
plot(node_num1, speedupFile3, 'b')
xlim([2 8])
xlabel('Number of Nodes')
ylabel('Speedup (T_1/T_N)')
legend('1 GB Input Files', '2 GB Input Files', '2.8 GB Input Files', 'Location','southeast')

%% Comparison
Hybrid = [14879.622630 29821.245250 40248.743350 52541.56599 66237.22636];

MapReduce = [27.320309 54.217165 6686.974704 25786.847294 60487.95392];

fileSize = [1 2 2.8 3.8 4.7];

plot(fileSize, Hybrid, fileSize, MapReduce)
xlim([1 4.7])
xlabel('File size (GB)')
ylabel('Time (s)')
legend('Hybrid Solution', 'MapReduce Solution', 'Location','southeast')

%% Scalability plot

serialTime1 = 387594.1888;
serialTime2 = 712188.3784;
serialTime2_8 = 991263.7304;
gb1 = [44699.273660 33726.023210 25900.586940 22358.448720 16967.617330 15804.263070 14879.622630];
%           2      3         4           5         6           7        8
gb2 = [86398.547330 65452.046420 50801.173890 42716.897430 32935.234660 30108.526410 29821.245250];

gb2_8 = [120157.96630 91432.864980 70521.64344 55603.656410 45509.328520 42251.936590 40248.743350];

scalability1 = serialTime1 ./ gb1;
scalability2 = serialTime2 ./ gb2;
scalability2_8 = serialTime2_8 ./ gb2_8;
node_num1 = [2 3 4 5 6 7 8];
plot(node_num1, scalability1, node_num1, scalability2,node_num1, scalability2_8)
xlabel('Number of Nodes')
ylabel('Scalability (T_s/T_N)')
legend('1 GB Input Files', '2 GB Input Files', '2.8 GB Input Files', 'Location','southeast')