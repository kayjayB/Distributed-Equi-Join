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
gb1Log = log10(gb1);
gb2Log = log10(gb2);
gb2_8Log = log10(gb2_8);
threadNum1 = [1 4 8 16];
loglog(threadNum, gb1, 'r')
hold on 
loglog(threadNum, gb2, 'k')
hold on 
loglog(threadNum1, gb2_8, 'b')
xlim([0 16])
xlabel('Number of Threads')
ylabel('Time (s)')
legend('1 GB Input Files', '2 GB Input Files', '2.8 GB Input Files')

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