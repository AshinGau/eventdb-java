package org.osv.eventdb;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.osv.eventdb.fits.FitsQueryClient;
import org.osv.eventdb.fits.FitsQueryFormater;
import org.osv.eventdb.fits.HeQueryClient;
import org.osv.eventdb.fits.HeQueryFormater;
import org.osv.eventdb.fits.util.HeEventDecoder;
import org.osv.eventdb.hbase.MDQuery;
import org.osv.eventdb.util.ConfigProperties;

public class ExperimentTest {
	@Test
	public void testFunc() throws Exception {
		// 配置文件默认读取 /opt/eventdb/config.properties
		// 配置文件对象
		ConfigProperties conp = new ConfigProperties("/opt/eventdb/config.properties");
		// 生成多维查询数据库对象
		MDQuery md = new MDQuery(conp, "tableName");
		// 生成多维查询客户端
		FitsQueryClient client = new HeQueryClient(md);

		// 查询条件对象 FitsQueryFormater 可以设置查询条件
		FitsQueryFormater formater = new HeQueryFormater();
		// 时间范围设置，参数是开始时间，和结束时间，doule类型
		formater.setTimeRange(178797000.0, 178797005.0);
		// 设置属性查询条件：范围查询, 参数都是字符串，例子： formater.setPropertyRange("detID", "2", "13")
		formater.setPropertyRange("channel", "2", "120");
		// 设置属性查询条件：范围列表
		List<String> detID = new ArrayList<String>();
		detID.add("1");
		detID.add("3");
		detID.add("5");
		detID.add("7");
		detID.add("9");
		formater.setPropertyList("detID", detID);

		List<byte[]> result = client.query(formater);
		List<HeEventDecoder.He> heList = HeEventDecoder.decode(result);

		for (HeEventDecoder.He he : heList) {
			System.out.printf("%f\t%d\t%d\t%d\t%d\n", he.time, he.detID & 0x00ff, he.channel & 0x00ff,
					he.pulse & 0x00ff, he.eventType & 0x00ff);
		}
		System.out.printf("time\t\t\tdetID\tchannel\tpulse\teventType\n");
	}
}