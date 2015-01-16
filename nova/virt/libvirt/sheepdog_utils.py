# Copyright 2014 hengtianyun, Inc
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import urllib

from oslo.config import cfg

from nova import exception
from nova.i18n import _
from nova.i18n import _LE
from nova.i18n import _LW
from oslo.utils import excutils
from oslo.utils import units
from nova.openstack.common import log as logging
from nova import utils

LOG = logging.getLogger(__name__)

sheepdog_opts = [
	    cfg.StrOpt('sheepdog_instance_prefix', default='instance_',
		       help='Prefix for instance names for sheepdog backend.'),
	    cfg.StrOpt('sheepdog_host', default='localhost',
		       help='IP for sheepdog service.'),
	    cfg.IntOpt('sheepdog_port', default=7000,
		       help='Port for sheepdog service.'),
	]
sheepdog_group = cfg.OptGroup(name='sheepdog',
                          title='sheepdog opts')

CONF = cfg.CONF

CONF.register_opts(sheepdog_opts, 'sheepdog')

def execute(*args, **kwargs):
    return utils.execute(*args, **kwargs)

def sheepdog_execute(*args, **kwargs):
    """Add sheepdog connection info to commands."""
    
    options = ('-a', CONF.sheepdog.sheepdog_host, '-p',
               CONF.sheepdog.sheepdog_port)
    args += options
    return execute(*args, **kwargs)

def get_vdi_names():
    '''get all vdi name from sheepdog
    '''
    out, err = sheepdog_execute('dog', 'vdi', 'list')
    lines = [line.strip().split() for line in out.splitlines()]
    tags = set(['s', 'c'])
    return [l[0] if l[0] not in tags else l[1] for l in lines[1:]]

def get_vdi_size(vdi_name):
    '''get the special vdi's size
    :vdi_name: vdi name
    '''

    out, err = sheepdog_execute('dog', 'vdi', 'list')
    lines = [line.strip().split() for line in out.splitlines()]
    tags = set(['s', 'c'])
    name_size = [(l[0],l[2],l[3]) if l[0] not in tags else (l[1],l[3],l[4]) for l in lines[1:]]
    for i in name_size:
	if vdi_name == i[0]:
	    if i[2] == "GB":
	        return float(i[1]) * units.Gi
            elif i[2] == "MB":
                return float(i[1]) * units.Mi
    return -1 
    
def get_sheepdog_prefix():
    return CONF.sheepdog.sheepdog_instance_prefix 
   
class SheepdogDriver(object):

    def __init__(self):
	self.sheepdog_instance_prefix = CONF.sheepdog.sheepdog_instance_prefix
	self.sheepdog_host = CONF.sheepdog.sheepdog_host
	self.sheepdog_port = CONF.sheepdog.sheepdog_port


    def clone(self, vdi_name, snapshot_name, new_vdi_name):
        sheepdog_execute('dog', 'vdi', 'clone','-s',snapshot_name, vdi_name, new_vdi_name)
	
    def size(self, vdi_name):
	size = get_vdi_size(vdi_name)
	return size

    def snapshot(self, vdi_name, snapshot_tag):
        sheepdog_execute('dog', 'vdi', 'snapshot', vdi_name,'-s'+snapshot_tag)

    def resize(self, vdi_name, size):

        LOG.debug('resizing sheepdog image %s to %d', vdi_name, size)
        sheepdog_execute('dog', 'vdi', 'resize', vdi_name, size)
    
    def create(self, vdi_name, size):
	
        sheepdog_execute('dog', 'vdi', 'create', vdi_name, size)

	    	
    def exists(self, vdi_name):
        for vdi in get_vdi_names():
            if vdi_name == vdi:
                return True
	return False

    def delete(self, vdi_name, snapshot_tag = None):
        '''delete vdi or snapshot
        :vdi_name: vdi name
        :snapshot_tag: if snapshot_tag not None deleting snapshot or delete the vdi
        '''

        LOG.debug("Deleting vdi %s",vdi_name)
	if snapshot_tag:
	    sheepdog_execute('dog', 'vdi', 'delete', '-s', snapshot_tag, vdi_name)
	else:
	    sheepdog_execute('dog', 'vdi', 'delete',  vdi_name)

    def cleanup_volumes(self, instance):
        def belongs_to_instance(disk):
            return disk.startswith(instance['uuid'])
        volumes = get_vdi_names()
        for volume in filter(belongs_to_instance, volumes):
            self.delete(volume)
