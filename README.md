
# Swift-on-HPSS
Swift-on-HPSS is a fork of the Swift-on-File Swift Object Server implementation
that enables users to access the same data, both as an object and as a file.
Data can be stored and retrieved through Swift's REST interface or as files from
your site's HPSS archive system.

Swift-on-HPSS is to be deployed as a Swift [storage policy](http://docs.openstack.org/developer/swift/overview_policies.html),
which provides the advantages of being able to extend an existing Swift cluster
and also migrating data to and from policies with different storage backends.

The main difference from the default Swift Object Server is that Swift-on-HPSS
stores objects following the same path hierarchy as the object's URL. In contrast,
the default Swift implementation stores the object following the mapping given
by the Ring, and its final file path is unknown to the user.

For example, an object with URL: `https://swift.example.com/v1/acct/cont/obj`,
would be stored the following way by the two systems:
* Swift: `/mnt/sdb1/2/node/sdb2/objects/981/f79/f566bd022b9285b05e665fd7b843bf79/1401254393.89313.data`
* SwiftOnHPSS: `/mnt/swiftonhpss/acct/cont/obj`

## Use cases
Swift-on-HPSS can be especially useful in cases where access over multiple
protocols is desired. For example, imagine a deployment where collected weather
datasets are uploaded as objects over Swift's REST interface and existing weather
modelling software can pull this archived weather data using any number of interfaces
HPSS already supports.

Along the same lines, data can be ingested over Swift's REST interface and then
analytic software like Hadoop can operate directly on the data without having to
move the data to a separate location.

Another use case is where users might need to migrate data from an existing file
storage systems to a Swift cluster.

Similarly, scientific applications may process file data and then select some or all
of the data to publish to outside users through the swift interface.

## Limitations and Future plans
Currently, files added over a file interface (e.g., PFTP or FUSE), do not show
up in container listings, still those files would be accessible over Swift's REST
interface with a GET request. We are working to provide a solution to this limitation.

There is also subtle but very important difference in the implementation of
[last write wins](doc/markdown/last_write_wins.md) behaviour when compared to
OpenStack Swift.

Because Swift-On-HPSS relies on the data replication support of HPSS
(dual/quad copy, RAIT, etc) the Swift Object replicator process does not have
any role for containers using the Swift-on-HPSS storage policy.
This means that Swift geo replication is not available to objects in
in containers using the Swift-on-HPSS storage policy.
Multi-site replication for these objects must be provided by your HPSS
configuration.
 
## Get involved:
(TODO: write specifics for Swift-on-HPSS)

To learn more about Swift-On-File, you can watch the presentation given at
the Paris OpenStack Summit: [Deploying Swift on a File System](http://youtu.be/vPn2uZF4yWo).
The Paris presentation slides can be found [here](https://github.com/thiagol11/openstack-fall-summit-2014)
Also see the presentation given at the Atlanta Openstack Summit: [Breaking the Mold with Openstack Swift and GlusterFS](http://youtu.be/pSWdzjA8WuA).
The Atlanta presentation slides can be found [here](http://lpabon.github.io/openstack-summit-2014).

Join us in contributing to the project. Feel free to file bugs, help with documentation
or work directly on the code. You can file bugs or blueprints on [launchpad](https://launchpad.net/swiftonfile)

or find us in the #swiftonfile channel on Freenode.

# Guides to get started:
(TODO: modify these guides with Swift-on-HPSS specifics)

1. [Quick Start Guide with XFS/GlusterFS](doc/markdown/quick_start_guide.md)
2. [Developer Guide](doc/markdown/dev_guide.md)
