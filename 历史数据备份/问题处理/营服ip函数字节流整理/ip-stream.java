private static byte[] getDbData(String filename) {
        try {
            InputStream inputStream = IPRegion.class.getClassLoader().getResourceAsStream(filename);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];

            int n;
            while((n = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, n);
            }

            inputStream.close();
            byte[] data = out.toByteArray();
            return data;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static DbSearcher mySearcherIpv4 = new DbSearcher(getDbData("ipv4.db"), QueryType.MEMORY);

    private static DbSearcher mySearcherIpv6 = new DbSearcher(getDbData("ipv6.db"), QueryType.MEMORY);

    mvn install:install-file -DgroupId=org.lionsoul -DartifactId=ip2region-stream -Dversion=1.1 -Dpackaging=jar -Dfile=F:\Users\19406\Desktop\工作\2、数据开发\2、在线云迁移相关文件\6、自定义udf函数\ipudfjar包\ip2region-maker-1.0.jar