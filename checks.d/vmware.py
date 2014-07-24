# project
from checks import AgentCheck

# 3p
from pysphere import (
    VIServer,
    MORTypes,
    VIMor,
)

NO_TAGS = "no_tags"

class VMWareMetric(object):
    def __init__(self, group, counter, tagged=None):
        self.group = group
        self.counter = counter
        self.tagged = tagged

    def get_stat_name(self):
        return "{0}.{1}".format(self.group, self.counter)

    def get_dd_metric_name(self):
        return "{0}.{1}.{2}".format("vmware", self.group, self.counter)

    def get_dd_metrics_from_vmwarestats(self, statistics):
        metrics = []
        for s in statistics:
            if s.group == self.group and s.counter == self.counter:
                # We only keep the metrics tagged per instance
                if self.tagged and s.instance:
                    val, m_type = VMWareMetric.transform_to_datadog(s.value, s.unit)
                    metrics.append((
                        self.get_dd_metric_name(),
                        val,
                        m_type,
                        "{0}:{1}".format(self.tagged, s.instance)
                    ))
                elif not self.tagged and not s.instance:
                    val, m_type = VMWareMetric.transform_to_datadog(s.value, s.unit)
                    metrics.append((
                        self.get_dd_metric_name(),
                        val,
                        m_type,
                        None
                    ))

        return metrics

    @classmethod
    def transform_to_datadog(cls, value, unit):
        if unit == "percent":
            return float(value)/100, "gauge"
        if unit == "kiloBytes":
            return int(value)*1024, "gauge"
        if unit in ("number", "joule", "watt"):
            return int(value), "gauge"
        else:
            raise Exception("Don't know how to transform this VMWare metric into a Datadog one")


class VMWare(AgentCheck):
    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.connections = {}
        self.performance_managers = {}

    def _generate_instance_key(self, instance):
        return (instance.get('host'), instance.get('port'))

    def _get_conn_pm(self, instance):
        key = self._generate_instance_key(instance)
        if key not in self.connections:
            try:
                self.connections[key] = VIServer()
                self.connections[key].connect(instance.get('host'), instance.get('username'), instance.get('password'))
                self.performance_managers[key] = self.connections[key].get_performance_manager()
            except:
                raise Exception("Problem when connecting to server")

        return self.connections[key], self.performance_managers[key]

    def _check_hosts(self, instance):
        conn, pm = self._get_conn_pm(instance)
        hostnames = conn.get_hosts()
        hosts = [ VIMor(k, MORTypes.HostSystem) for k in hostnames.keys() ]

        vmware_metrics = [
            VMWareMetric('cpu', 'coreUtilization'),
            VMWareMetric('cpu', 'usage'),
            VMWareMetric('cpu', 'utilization'),
            VMWareMetric('mem', 'active'),
            VMWareMetric('mem', 'compressed'),
            VMWareMetric('mem', 'consumed'),
            VMWareMetric('mem', 'granted'),
            VMWareMetric('mem', 'shared'),
            VMWareMetric('mem', 'sysUsage'),
            VMWareMetric('mem', 'swapused'),
            VMWareMetric('mem', 'usage'),
            VMWareMetric('power', 'power'),
            VMWareMetric('power', 'energy'),
            VMWareMetric('net', 'packetsRx', tagged='nic'),
            VMWareMetric('net', 'packetsTx', tagged='nic'),
            VMWareMetric('net', 'multicastRx', tagged='nic')
        ]


        for h in hosts:
            dd_metrics = []

            statistics = pm.get_entity_statistic(h, [ m.get_stat_name() for m in vmware_metrics])
            for vmware_metric in vmware_metrics:
                dd_metrics += vmware_metric.get_dd_metrics_from_vmwarestats(statistics)

            for m_name, m_value, m_type, m_tag in dd_metrics:
                m_tags = []
                if m_tag:
                    m_tags.append(m_tag)
                metric_method = getattr(self, m_type)
                metric_method(m_name, m_value, hostname=hostnames[h], tags=m_tags)


    def check(self, instance):
        conn, pm = self._get_conn_pm(instance)
        self._check_hosts(instance)

if __name__ == '__main__':
    check, instances = VMWare.from_yaml('conf.d/vmware.yaml')
    for instance in instances:
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())
