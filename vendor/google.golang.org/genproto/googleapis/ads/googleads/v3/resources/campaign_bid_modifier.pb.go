// Code generated by protoc-gen-go. DO NOT EDIT.
// source: google/ads/googleads/v3/resources/campaign_bid_modifier.proto

package resources

import (
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
	wrappers "github.com/golang/protobuf/ptypes/wrappers"
	common "google.golang.org/genproto/googleapis/ads/googleads/v3/common"
	_ "google.golang.org/genproto/googleapis/api/annotations"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// Represents a bid-modifiable only criterion at the campaign level.
type CampaignBidModifier struct {
	// Immutable. The resource name of the campaign bid modifier.
	// Campaign bid modifier resource names have the form:
	//
	// `customers/{customer_id}/campaignBidModifiers/{campaign_id}~{criterion_id}`
	ResourceName string `protobuf:"bytes,1,opt,name=resource_name,json=resourceName,proto3" json:"resource_name,omitempty"`
	// Output only. The campaign to which this criterion belongs.
	Campaign *wrappers.StringValue `protobuf:"bytes,2,opt,name=campaign,proto3" json:"campaign,omitempty"`
	// Output only. The ID of the criterion to bid modify.
	//
	// This field is ignored for mutates.
	CriterionId *wrappers.Int64Value `protobuf:"bytes,3,opt,name=criterion_id,json=criterionId,proto3" json:"criterion_id,omitempty"`
	// The modifier for the bid when the criterion matches.
	BidModifier *wrappers.DoubleValue `protobuf:"bytes,4,opt,name=bid_modifier,json=bidModifier,proto3" json:"bid_modifier,omitempty"`
	// The criterion of this campaign bid modifier.
	//
	// Types that are valid to be assigned to Criterion:
	//	*CampaignBidModifier_InteractionType
	Criterion            isCampaignBidModifier_Criterion `protobuf_oneof:"criterion"`
	XXX_NoUnkeyedLiteral struct{}                        `json:"-"`
	XXX_unrecognized     []byte                          `json:"-"`
	XXX_sizecache        int32                           `json:"-"`
}

func (m *CampaignBidModifier) Reset()         { *m = CampaignBidModifier{} }
func (m *CampaignBidModifier) String() string { return proto.CompactTextString(m) }
func (*CampaignBidModifier) ProtoMessage()    {}
func (*CampaignBidModifier) Descriptor() ([]byte, []int) {
	return fileDescriptor_6a7c4dd3c8a563d8, []int{0}
}

func (m *CampaignBidModifier) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CampaignBidModifier.Unmarshal(m, b)
}
func (m *CampaignBidModifier) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CampaignBidModifier.Marshal(b, m, deterministic)
}
func (m *CampaignBidModifier) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CampaignBidModifier.Merge(m, src)
}
func (m *CampaignBidModifier) XXX_Size() int {
	return xxx_messageInfo_CampaignBidModifier.Size(m)
}
func (m *CampaignBidModifier) XXX_DiscardUnknown() {
	xxx_messageInfo_CampaignBidModifier.DiscardUnknown(m)
}

var xxx_messageInfo_CampaignBidModifier proto.InternalMessageInfo

func (m *CampaignBidModifier) GetResourceName() string {
	if m != nil {
		return m.ResourceName
	}
	return ""
}

func (m *CampaignBidModifier) GetCampaign() *wrappers.StringValue {
	if m != nil {
		return m.Campaign
	}
	return nil
}

func (m *CampaignBidModifier) GetCriterionId() *wrappers.Int64Value {
	if m != nil {
		return m.CriterionId
	}
	return nil
}

func (m *CampaignBidModifier) GetBidModifier() *wrappers.DoubleValue {
	if m != nil {
		return m.BidModifier
	}
	return nil
}

type isCampaignBidModifier_Criterion interface {
	isCampaignBidModifier_Criterion()
}

type CampaignBidModifier_InteractionType struct {
	InteractionType *common.InteractionTypeInfo `protobuf:"bytes,5,opt,name=interaction_type,json=interactionType,proto3,oneof"`
}

func (*CampaignBidModifier_InteractionType) isCampaignBidModifier_Criterion() {}

func (m *CampaignBidModifier) GetCriterion() isCampaignBidModifier_Criterion {
	if m != nil {
		return m.Criterion
	}
	return nil
}

func (m *CampaignBidModifier) GetInteractionType() *common.InteractionTypeInfo {
	if x, ok := m.GetCriterion().(*CampaignBidModifier_InteractionType); ok {
		return x.InteractionType
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*CampaignBidModifier) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*CampaignBidModifier_InteractionType)(nil),
	}
}

func init() {
	proto.RegisterType((*CampaignBidModifier)(nil), "google.ads.googleads.v3.resources.CampaignBidModifier")
}

func init() {
	proto.RegisterFile("google/ads/googleads/v3/resources/campaign_bid_modifier.proto", fileDescriptor_6a7c4dd3c8a563d8)
}

var fileDescriptor_6a7c4dd3c8a563d8 = []byte{
	// 534 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x53, 0x4f, 0x8b, 0xd3, 0x40,
	0x14, 0xb7, 0xed, 0x56, 0xdc, 0x69, 0x45, 0x89, 0x97, 0x58, 0x17, 0xdd, 0x2a, 0x0b, 0x2b, 0xe8,
	0x0c, 0x98, 0xc5, 0x43, 0x44, 0x64, 0xb2, 0xc2, 0x5a, 0x41, 0x59, 0xa2, 0x14, 0x94, 0x42, 0x98,
	0x64, 0xa6, 0x71, 0xa0, 0x99, 0x89, 0x33, 0x69, 0x65, 0x91, 0x3d, 0xfa, 0x45, 0x3c, 0xfa, 0x3d,
	0xbc, 0xf8, 0x29, 0x7a, 0xde, 0x8f, 0xb0, 0x27, 0x49, 0x32, 0x99, 0xad, 0xd8, 0xba, 0x78, 0x7b,
	0xe1, 0xfd, 0xfe, 0xbc, 0xf7, 0xcb, 0x3c, 0xf0, 0x3c, 0x95, 0x32, 0x9d, 0x31, 0x44, 0xa8, 0x46,
	0x75, 0x59, 0x56, 0x0b, 0x0f, 0x29, 0xa6, 0xe5, 0x5c, 0x25, 0x4c, 0xa3, 0x84, 0x64, 0x39, 0xe1,
	0xa9, 0x88, 0x62, 0x4e, 0xa3, 0x4c, 0x52, 0x3e, 0xe5, 0x4c, 0xc1, 0x5c, 0xc9, 0x42, 0x3a, 0xc3,
	0x9a, 0x03, 0x09, 0xd5, 0xd0, 0xd2, 0xe1, 0xc2, 0x83, 0x96, 0x3e, 0x78, 0xbc, 0xc9, 0x21, 0x91,
	0x59, 0x26, 0x05, 0x4a, 0x14, 0x2f, 0x98, 0xe2, 0xa4, 0x56, 0x1c, 0xdc, 0x6b, 0xe0, 0x39, 0x47,
	0x53, 0xce, 0x66, 0x34, 0x8a, 0xd9, 0x27, 0xb2, 0xe0, 0xd2, 0x58, 0x0e, 0x6e, 0xaf, 0x00, 0x1a,
	0x17, 0xd3, 0xba, 0x6b, 0x5a, 0xd5, 0x57, 0x3c, 0x9f, 0xa2, 0x2f, 0x8a, 0xe4, 0x39, 0x53, 0xda,
	0xf4, 0x77, 0x56, 0xa8, 0x44, 0x08, 0x59, 0x90, 0x82, 0x4b, 0x61, 0xba, 0xf7, 0x7f, 0x6e, 0x81,
	0x5b, 0x87, 0x66, 0xd7, 0x80, 0xd3, 0x37, 0x66, 0x53, 0xe7, 0x03, 0xb8, 0xde, 0xf8, 0x44, 0x82,
	0x64, 0xcc, 0x6d, 0xed, 0xb6, 0xf6, 0xb7, 0x83, 0x83, 0x25, 0xee, 0x9e, 0x63, 0x08, 0x1e, 0x5d,
	0xec, 0x6d, 0xaa, 0x9c, 0x6b, 0x98, 0xc8, 0x0c, 0xad, 0x11, 0x0b, 0xfb, 0x8d, 0xd4, 0x5b, 0x92,
	0x31, 0x27, 0x01, 0xd7, 0x9a, 0x74, 0xdd, 0xf6, 0x6e, 0x6b, 0xbf, 0xf7, 0x64, 0xc7, 0x88, 0xc0,
	0x66, 0x07, 0xf8, 0xae, 0x50, 0x5c, 0xa4, 0x63, 0x32, 0x9b, 0xb3, 0xe0, 0xe1, 0x12, 0x77, 0xce,
	0xf1, 0x03, 0x30, 0xbc, 0xd4, 0x33, 0xb4, 0xc2, 0xce, 0x21, 0xe8, 0x9b, 0x8c, 0xa5, 0x88, 0x38,
	0x75, 0x3b, 0x95, 0xd1, 0x9d, 0xbf, 0x8c, 0x46, 0xa2, 0x78, 0x7a, 0x50, 0xfb, 0x74, 0x96, 0xb8,
	0x13, 0xf6, 0x2c, 0x6b, 0x44, 0x9d, 0x17, 0xa0, 0xbf, 0xfa, 0xfb, 0xdd, 0xad, 0x0d, 0xd3, 0xbe,
	0x94, 0xf3, 0x78, 0xc6, 0x2a, 0x95, 0xb0, 0x17, 0xaf, 0xa4, 0xc8, 0xc0, 0x4d, 0x2e, 0x0a, 0xa6,
	0x48, 0x52, 0x66, 0x1e, 0x15, 0x27, 0x39, 0x73, 0xbb, 0x95, 0x88, 0x07, 0x37, 0x3d, 0xa2, 0xfa,
	0x85, 0x94, 0x83, 0x35, 0xbc, 0xf7, 0x27, 0x39, 0x1b, 0x89, 0xa9, 0x2c, 0x27, 0xec, 0xbe, 0xba,
	0x12, 0xde, 0xe0, 0x7f, 0xf6, 0xfc, 0xe2, 0x0c, 0x7f, 0xfe, 0xbf, 0x5f, 0xe2, 0xe0, 0x64, 0xae,
	0x0b, 0x99, 0x31, 0xa5, 0xd1, 0xd7, 0xa6, 0x3c, 0xb5, 0xaf, 0x7e, 0x05, 0x59, 0xf6, 0xd7, 0xdd,
	0xc2, 0x69, 0xd0, 0x03, 0xdb, 0x36, 0xac, 0xe0, 0x5b, 0x1b, 0xec, 0x25, 0x32, 0x83, 0x97, 0x9e,
	0x46, 0xe0, 0xae, 0x19, 0xe7, 0xb8, 0x4c, 0xf2, 0xb8, 0xf5, 0xf1, 0xb5, 0xa1, 0xa7, 0x72, 0x46,
	0x44, 0x0a, 0xa5, 0x4a, 0x51, 0xca, 0x44, 0x95, 0x33, 0xba, 0x58, 0xea, 0x1f, 0x77, 0xfb, 0xcc,
	0x56, 0xdf, 0xdb, 0x9d, 0x23, 0x8c, 0x7f, 0xb4, 0x87, 0x47, 0xb5, 0x24, 0xa6, 0x1a, 0xd6, 0x65,
	0x59, 0x8d, 0x3d, 0x18, 0x36, 0xc8, 0x5f, 0x0d, 0x66, 0x82, 0xa9, 0x9e, 0x58, 0xcc, 0x64, 0xec,
	0x4d, 0x2c, 0xe6, 0xac, 0xbd, 0x57, 0x37, 0x7c, 0x1f, 0x53, 0xed, 0xfb, 0x16, 0xe5, 0xfb, 0x63,
	0xcf, 0xf7, 0x2d, 0x2e, 0xbe, 0x5a, 0x0d, 0xeb, 0xfd, 0x0e, 0x00, 0x00, 0xff, 0xff, 0x5c, 0x0c,
	0xc5, 0xc5, 0x63, 0x04, 0x00, 0x00,
}