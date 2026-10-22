# Legacy protocol fixture

worker_v1_0_13.pb is a FileDescriptorSet generated from worker.proto and
common.proto at public upstream commit 5a75638764d856fecdfd7cf540c9830076dfdefc
(v1.0.13), https://github.com/alibaba/schedulerx-worker-go.
It is test data only. Existing copyright and license remain in the repository.

Reproduce with protoc --include_imports --descriptor_set_out=worker_v1_0_13.pb
worker.proto, using those two original source files and their directory as
--proto_path. No current descriptor is used as the old-schema oracle.

worker_v1_0_10.pb uses the same command with v1.0.10 sources from upstream
commit 4d8738440e0327a48b17a95ff73a4ae4934de7e0. This is the SDK version
currently declared by ndr-scheduler and ndr-scheduler-region.
